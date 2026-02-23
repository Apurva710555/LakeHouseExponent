import os
import pandas as pd
import requests
from flask import Flask, request, render_template, jsonify
from flask_mail import Mail, Message
from dotenv import load_dotenv
from dbx_auth import get_dbx_access_token
from handlers import *
from logger_utils import (
    append_audit,
    read_audit,
    set_audit_context,
    clear_audit_context,
)
from datetime import datetime
from databricks_api import scim_me_workspace

# NEW: Import OpenAI client for query optimization - Query Data
# from openai import OpenAI ----Trying to inetgrate gemini
import google.generativeai as genai


print("------------SP token-------------")
print(get_dbx_access_token()[:20])
print("------------SP token-------------")

# Load environment variables early
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"[INFO] Loaded .env from {dotenv_path}")
else:
    print("[WARN] .env file not found — relying on Databricks environment variables")

DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
WORKSPACE_INSTANCE = os.getenv("WORKSPACE_INSTANCE")
ORG_ID = os.getenv("ORG_ID")

# if not DATABRICKS_INSTANCE or not DATABRICKS_TOKEN or not WORKSPACE_INSTANCE or not ORG_ID:
#     print("⚠️ Warning: DATABRICKS_INSTANCE or DATABRICKS_TOKEN or WORKSPACE_INSTANCE or ORG_ID not found in environment!")
if not WORKSPACE_INSTANCE or not ORG_ID:
    raise RuntimeError("Missing WORKSPACE_INSTANCE or ORG_ID")
else:
    print(f"[INFO] Using Databricks workspace: {DATABRICKS_INSTANCE}")


app = Flask(__name__, template_folder="templates")
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change_me")
# app.config["MAIL_DEFAULT_SENDER"] = os.getenv(
#     "EMAIL_FROM", os.getenv("MAIL_DEFAULT_SENDER", "")
# )
app.config["MAIL_SERVER"] = os.getenv(
    "SMTP_SERVER", os.getenv("MAIL_SERVER", "smtp.example.com")
)
app.config["MAIL_PORT"] = int(os.getenv("SMTP_PORT", os.getenv("MAIL_PORT", 587)))
app.config["MAIL_USE_TLS"] = (
    True
    if str(os.getenv("MAIL_USE_TLS", "true")).lower == "true"
    or str(os.getenv("SMTP_PORT", 587)) == "587"
    else False
)
app.config["MAIL_USERNAME"] = os.getenv("SMTP_USER", os.getenv("MAIL_USERNAME", ""))
app.config["MAIL_PASSWORD"] = os.getenv("SMTP_PASS", os.getenv("MAIL_PASSWORD", ""))
app.config["MAIL_DEFAULT_SENDER"] = os.getenv(
    "EMAIL_FROM", os.getenv("MAIL_DEFAULT_SENDER", "")
)

mail = Mail(app)
# NEW: Initialize OpenAI client for AI-based query optimization - Query Data
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) // ----Trying to inetgrate gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

model = genai.GenerativeModel("models/gemini-2.5-flash")

# @app.route("/")
# def home():
#     """Render upload form."""
#     return render_template("index.html")


@app.route("/")
def home():
    return render_template(
        "index.html",
        DATABRICKS_HOST=WORKSPACE_INSTANCE,
    )


@app.route("/run", methods=["POST"])
def run_action():
    """Handle file upload or single-entry form."""
    results = []
    clear_audit_context()
    # Set request-scoped audit context
    run_id = (
        f"sync_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{os.urandom(4).hex()}"
    )
    set_audit_context(
        run_id=run_id,
        run_by=request.headers.get("X-User-Email") or os.getenv("RUN_BY") or "",
    )

    # Case 1: File upload
    if "file" in request.files and request.files["file"].filename:
        f = request.files["file"]
        ext = os.path.splitext(f.filename)[1].lower()
        set_audit_context(file_path=f.filename)
        if ext in [".xls", ".xlsx"]:
            df = pd.read_excel(f)
        else:
            df = pd.read_csv(f)

        for ix, row in df.fillna("").to_dict(orient="index").items():
            set_audit_context(row_id=ix, request_payload=row)
            results.append(process_row(ix, row))

    # Case 2: Manual entry form
    else:
        # Build row from form; use first/last name only
        row = {
            "admin": request.form.get("admin", ""),
            "action": request.form.get("action", ""),
            "principal_type": request.form.get("principal_type", ""),
            "user_email": request.form.get("user_email", ""),
            "first_name": request.form.get("first_name", ""),
            "last_name": request.form.get("last_name", ""),
            "group_members": request.form.get("group_members", ""),
            "domain": request.form.get("domain", ""),
            "bu": request.form.get("bu", ""),
            "other_type": request.form.get("other_type", ""),
            "role": request.form.get("role", ""),
            "appName": request.form.get("appName", ""),
            "env": request.form.get("env", ""),
        }
        set_audit_context(
            admin=row.get("admin"), file_path="", row_id=0, request_payload=row
        )
        results.append(process_row(0, row))

    # Build acknowledgment summary
    total = len(results)
    failures = 0
    for r in results:
        res = r.get("result") or {}
        if isinstance(res, dict) and res.get("error"):
            failures += 1

    if total == 1:
        r0 = results[0]
        actor = (r0.get("principal_type") or "").title() or "Operation"
        act = r0.get("action") or ""
        ident = r0.get("identifier") or "request"
        if failures:
            message = f"{actor} {act} failed for {ident}."
        else:
            message = f"{actor} {act} submitted successfully for {ident}."
    else:
        if failures:
            message = f"Processed {total} records: {total - failures} succeeded, {failures} failed."
        else:
            message = f"Processed {total} records successfully."

    return render_template("result.html", results=results, message=message)


def process_row(ix, row):
    """Run the requested action safely."""
    action = (row.get("action") or "").strip().upper()
    try:
        if action == "CREATE_USER":
            resp = create_user_handler(row)
        elif action == "DELETE_USER":
            resp = delete_user_handler(row)
        elif action == "UPDATE_USER":
            resp = update_user_handler(row)
        elif action == "CREATE_GROUP":
            resp = create_group_handler(row)
        elif action == "ADD_TO_GROUP":
            resp = add_members_to_group_handler(row)
        elif action == "REMOVE_FROM_GROUP":
            resp = remove_members_from_group_handler(row)
        else:
            append_audit(action, "unknown", "", "FAILED", "unknown action")
            resp = {"error": f"unknown action: {action}"}
    except Exception as e:
        identifier = str(
            row.get("user_email") or construct_group_name(row)
            if row.get("action")
            in ["CREATE_GROUP", "ADD_TO_GROUP", "REMOVE_FROM_GROUP", "UPDATE_GROUP"]
            else ""
        )
        append_audit(action, "error", identifier, "FAILED", str(e))
        resp = {"error": str(e)}

    return {
        "row": ix,
        "action": action,
        "principal_type": row.get("principal_type", ""),
        "identifier": row.get("user_email")
        or (
            construct_group_name(row)
            if action in ["CREATE_GROUP", "ADD_TO_GROUP", "REMOVE_FROM_GROUP"]
            else ""
        )
        or row.get("admin")
        or "",
        "result": resp,
    }


@app.route("/project-onboard", methods=["GET", "POST"])
def onboard_project():
    if request.method == "GET":
        return render_template("onboarding.html")
    if request.method == "POST":
        clear_audit_context()

        run_id = f"onboard_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{os.urandom(4).hex()}"
        run_by_email = request.headers.get("X-User-Email") or os.getenv("RUN_BY") or ""
        set_audit_context(run_id=run_id, run_by=run_by_email, action="ONBOARD_PROJECT")

        form_data = {
            "cluster_type": request.form.get("cluster_type"),
            "sql_wh_size": request.form.get("sql_wh_size"),
            "node_type_id": request.form.get("node_type_id"),
            "base_workers": request.form.get(
                "base_workers"
            ),  # Handled as string, converted in handler
            "department": request.form.get("department"),
            "organization_name": request.form.get("organization_name"),
            "application_owner_email": request.form.get("application_owner_email"),
            "application_name": request.form.get("application_name"),
            "application_owner": request.form.get("application_owner"),
            "business_owner": request.form.get("business_owner"),
            "cost_center": request.form.get("cost_center"),
            "environment": request.form.get("env", ""),
        }
        set_audit_context(row_id=0, request_payload=form_data)
        app_name = form_data.get("application_name") or "unknown_project"

        environment = form_data["environment"]
        print("[INFO] Selected Environment: ", environment)

        try:
            # Call the new handler
            results = onboard_project_handler(form_data)
            message = f"Project '{app_name}' Onboarded Successfully. Below are the details of the resources provisioned:"
            status_code = 200

        except Exception as e:
            # Log the failure in the audit trail
            append_audit("ONBOARD_PROJECT", "project", app_name, "FAILED", str(e))
            results = {"error": str(e)}
            message = f"Project Onboarding Failed for '{app_name}': {str(e)}"
            status_code = 500

        # Render a dedicated result page for onboarding
        return (
            render_template("result_po.html", result=results, message=message),
            status_code,
        )


@app.route("/audit", methods=["GET"])
def audit():
    """
    Get audit logs with optional filtering.
    Query parameters:
        - limit: Maximum number of records (default: 1000)
        - action: Filter by action (e.g., CREATE_USER)
        - status: Filter by status (e.g., SUCCESS, FAILED)
        - run_id: Filter by run ID
        - format: Response format (json or csv, default: json)
    """
    try:
        # Parse query parameters
        limit = int(request.args.get("limit", 1000))
        format_type = request.args.get("format", "json").lower()

        # Build filters
        filters = {}
        for key in ["action", "status", "run_id", "principal_type"]:
            value = request.args.get(key)
            if value:
                filters[key] = value

        # Read audit logs
        df = read_audit(limit=limit, filters=filters if filters else None)

        # Return in requested format
        if format_type == "csv":
            from flask import make_response

            csv_data = df.to_csv(index=False)
            response = make_response(csv_data)
            response.headers["Content-Disposition"] = (
                "attachment; filename=audit_log.csv"
            )
            response.headers["Content-Type"] = "text/csv"
            return response
        else:
            return jsonify(df.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# NEW: Test endpoint for email functionality - Query Data
@app.route("/api/test-email", methods=["GET"])
def test_email():
    try:
        send_kill_email("your.email@company.com", "TEST_QUERY_ID")
        return jsonify({"status": "sent"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/me", methods=["GET"])
def me():
    """Return current workspace user (email and displayName) using SCIM Me."""
    try:
        resp = scim_me_workspace()
        status = resp.get("status_code")
        body = resp.get("body") or {}
        if status != 200:
            return (
                jsonify({"error": True, "message": str(resp.get("error") or body)}),
                500,
            )
        # SCIM user typically has userName and emails list
        email = body.get("userName") or (body.get("emails") or [{}])[0].get("value")
        display = (
            body.get("displayName")
            or (body.get("name") or {}).get("formatted")
            or email
        )
        return jsonify({"email": email, "displayName": display})
    except Exception as e:
        return jsonify({"error": True, "message": str(e)}), 500


@app.route("/notify-owner", methods=["POST"])
def notify_owner():
    data = request.get_json()
    result = data.get("result", {})
    owner_email = result.get("application_owner_email") or os.getenv(
        "MAIL_DEFAULT_SENDER"
    )
    app_name = result.get("application_name")
    owner = result.get("application_owner")
    environment = result.get("environment")
    # workspace_prefix = f"deep.{environment}.cv.tatamotors"
    workspace_prefix = f"deep.{environment}.deep"

    workspace_url = f"https://{workspace_prefix}"

    table_rows = []
    if result.get("folder_path"):
        table_rows.append(
            f"<tr><td>Project Folder</td><td>{result.get('application_name', '')}</td><td>{result['folder_path']}</td><td>Success</td></tr>"
        )
    if result.get("cluster_id"):
        table_rows.append(
            f"<tr><td>All-Purpose Cluster</td><td>{result.get('application_name', '')}</td><td>{result['cluster_id']}</td><td>Success</td></tr>"
        )
    if result.get("sql_wh_id"):
        table_rows.append(
            f"<tr><td>SQL Warehouse</td><td>{result.get('application_name', '')}_reporting_wh</td><td>{result['sql_wh_id']}</td><td>Success</td></tr>"
        )
    if result.get("groups"):
        for group in result["groups"]:
            table_rows.append(
                f"<tr><td>Group</td><td>{group}</td><td>-</td><td>Success</td></tr>"
            )
    if result.get("uc_schema_full_name"):
        uc_schema_name = result["uc_schema_full_name"]
        uc_storage_root = result.get("uc_storage_root", "N/A")
        table_rows.append(
            f"<tr><td>Project Schema</td><td>{uc_schema_name}</td><td>{uc_storage_root}</td><td>Success</td></tr>"
        )
    if not table_rows:
        table_rows.append('<tr><td colspan="4">No Resources Found.</td></tr>')

    html_body = render_template(
        "email.html",
        owner=owner,
        app_name=app_name,
        workspace_url=workspace_url,
        table_rows=table_rows,
        result=result,
    )
    try:
        msg = Message(
            subject=f"DEEP Resources Provisioned: {app_name}",
            recipients=[owner_email],
            cc=["faguni.dhiman@exponentia.ai", "nethaji.kamalapuram@exponentia.ai"],
            html=html_body,
        )
        mail.send(msg)
        return jsonify({"message": "Notification Sent Successfully!"}), 200
    except Exception as e:
        return jsonify({"message": f"Failed to send email: {str(e)}"}), 500


# -------------------- GET ALL DASHBOARDS -------------------------
def get_dashboard_url(dashboard_id: str) -> str:
    """
    Returns the Lakeview dashboard URL from the given dashboard ID.
    """
    return f"{WORKSPACE_INSTANCE}/embed/dashboardsv3/{dashboard_id}?o={ORG_ID}"


@app.route("/api/list_dashboards", methods=["GET"])
def list_dashboards():
    domain = (request.args.get("domain") or "").strip().lower()
    print(f"domain: {domain}")

    dashboards = get_dashboards_list_handler()
    # print(f"dashboards_api_response: {dashboards}")
    df = pd.DataFrame(dashboards)

    # Extract "name" from dashboard_list (which is a dict)
    df["schema"] = (
        df["dashboard_list"]
        .apply(lambda x: (x.get("name") or "") if isinstance(x, dict) else "")
        .str.split(" ")
        .str[0]
    )

    # Filter by domain
    df_domain = df[df["schema"].str.lower() == domain]

    # Filter dashboards ending with "Monitoring"
    df_domain = df_domain[
        df_domain["dashboard_list"]
        .apply(lambda x: (x.get("name") or "") if isinstance(x, dict) else "")
        .str.endswith("Monitoring.")
    ]

    # print(f"df_domain['dashboard_list']: {df_domain['dashboard_list']}")
    # Build dashboard_url
    df_domain["dashboard_url"] = df_domain["dashboard_list"].apply(
        lambda x: get_dashboard_url(x.get("id")) if isinstance(x, dict) else None
    )

    filtered_dashboards = df_domain.to_dict(orient="records")
    print(f"filtered_dashboards: {filtered_dashboards}")

    return jsonify({"domain": domain, "dashboard_list": filtered_dashboards})


# job Monitoring
@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    try:
        headers = {
            "Authorization": f"Bearer {get_dbx_access_token()}",
            "Content-Type": "application/json",
        }
        url = f"{WORKSPACE_INSTANCE}/api/2.1/jobs/list"
        resp = requests.get(url, headers=headers, timeout=30)
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/jobs/run", methods=["POST"])
def run_job():
    try:
        job_id = request.json.get("job_id")
        if not job_id:
            return jsonify({"error": "job_id not provided"}), 400

        headers = {
            "Authorization": f"Bearer {get_dbx_access_token()}",
            "Content-Type": "application/json",
        }
        url = f"{WORKSPACE_INSTANCE}/api/2.1/jobs/run-now"
        resp = requests.post(url, headers=headers, json={"job_id": job_id}, timeout=30)
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/settings", methods=["GET"])
def settings():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        settings_path = os.path.join(base_dir, "docs", "settings.md")

        if not os.path.exists(settings_path):
            return "settings.md not found", 404

        with open(settings_path, "r", encoding="utf-8") as f:
            return f.read(), 200, {"Content-Type": "text/plain"}

    except Exception as e:
        return f"Error loading settings: {str(e)}", 500


@app.route("/help", methods=["GET"])
def help_doc():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, "docs", "help.md")

        if not os.path.exists(path):
            return "help.md not found", 404

        with open(path, "r", encoding="utf-8") as f:
            return f.read(), 200

    except Exception as e:
        return f"ERROR loading help: {str(e)}", 500


# NEW: API endpoint to terminate/kill a running SQL query - Query Data
@app.route("/api/query/kill", methods=["POST"])
def kill_query():
    try:
        print("[API] /api/query/kill called")

        data = request.get_json(silent=True) or {}
        query_id = data.get("query_id")
        user = data.get("user")
        # NEW: Get original query text for AI optimization - Query Data
        original_query = data.get("query_text")

        if not query_id:
            return jsonify({"status": "failed", "message": "query_id is required"}), 400

        # print(f"[API] Kill request for query_id={query_id}, user={user}")

        from databricks_api import cancel_sql_query

        # NEW: Call Databricks API to cancel the query
        result = cancel_sql_query(query_id, user)
        # print("[API] cancel_sql_query result:", result)

        if result.get("status") != "success":
            return jsonify(result), 409  # conflict / invalid state

        # NEW: ---- AI optimization (non-blocking) ---- - Query Data
        optimized_query = None

        try:
            if original_query:
                # print("inside if of original_query back")
                # NEW: Get AI-optimized version of the query - Query Data
                optimized_query = get_optimized_query_from_ai(original_query)
        except Exception as e:
            print("[WARN] AI optimization failed:", str(e))

        # NEW: ---- Email sending (non-blocking) ---- - Query Data
        try:
            # NEW: Send notification email with optimization suggestions - Query Data
            send_kill_email(
                to_email="faguni.dhiman@exponentia.ai",
                query_id=query_id,
                original_query=original_query,
                optimized_query=optimized_query,
            )
        except Exception as e:
            print("[WARN] Failed to send kill email:", str(e))

        # ✅ Kill succeeded regardless of AI/email
        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Query {query_id} cancelled successfully",
                }
            ),
            200,
        )

    except Exception as e:
        print("[API] Exception in kill_query:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# ------------------ LIST WORKSPACES ------------------
# this down part is for workspace name and filter part
@app.route("/api/workspaces", methods=["GET"])
def list_workspaces():
    try:
        headers = {
            "Authorization": f"Bearer {get_dbx_access_token()}",
            "Content-Type": "application/json",
        }

        ACCOUNT_HOST = "https://accounts.cloud.databricks.com"
        ACCOUNT_ID = os.getenv("ACCOUNT_ID")

        url = f"{ACCOUNT_HOST}/api/2.0/accounts/{ACCOUNT_ID}/workspaces"

        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()

        data = resp.json()

        if isinstance(data, dict):
            workspaces = data.get("workspaces", [])
        elif isinstance(data, list):
            workspaces = data
        else:
            workspaces = []

        result = [
            {
                "workspace_id": ws.get("workspace_id"),
                "workspace_name": ws.get("workspace_name"),
                "deployment_name": ws.get("deployment_name")
            }
            for ws in workspaces
        ]

        return jsonify(result)

    except Exception as e:
        print("Workspace fetch error:", str(e))
        return jsonify({"error": str(e)}), 500



@app.route("/api/query-status", methods=["GET"])
def get_query_status():
    try:
        # limit = int(request.args.get("limit", 5))
        print("Inside query status")
        workspace_id = request.args.get("workspace_id")
        workspace_instance = WORKSPACE_INSTANCE  # default workspace

        warehouse_id = request.args.get("warehouse_id")
         #  new part of workspace filter added here
        # workspace_id = request.args.get("workspace_id")   # ✅ ADD THIS LINE
        
        status_filter = request.args.get("status")
        # NEW: User filter to filter queries by username - Query Data
        user_filter = request.args.get("user")
        print("user_filter", user_filter)
        print("status_filter", status_filter)

        # NEW: Time-based filtering parameters - Query Data
        since_minutes = request.args.get("since_minutes", type=int)
        hours = request.args.get("hours", type=int, default=0)
        minutes = request.args.get("minutes", type=int, default=0)
        seconds = request.args.get("seconds", type=int, default=0)

        # print("since_minutes :", since_minutes)
        # if not warehouse_id:
        #     return jsonify({"error": "warehouse_id is required"}), 400

        # ✅ FIX: normalize status filter

        if status_filter and status_filter.upper() == "ALL":
            status_filter = None
        
        # =========================
            # WORKSPACE SWITCHING LOGIC (NEW) for worspace filter 17/02
        # =========================

        workspace_url = WORKSPACE_INSTANCE  # default

        if workspace_id:
            ACCOUNT_HOST = "https://accounts.cloud.databricks.com"
            ACCOUNT_ID = os.getenv("ACCOUNT_ID")

            headers = {
                "Authorization": f"Bearer {get_dbx_access_token()}",
                "Content-Type": "application/json",
            }

            ws_url = f"{ACCOUNT_HOST}/api/2.0/accounts/{ACCOUNT_ID}/workspaces/{workspace_id}"
            ws_resp = requests.get(ws_url, headers=headers, timeout=20)
            ws_resp.raise_for_status()

            workspace_data = ws_resp.json()
            deployment_name = workspace_data.get("deployment_name")

            if deployment_name:
                workspace_instance = f"https://{deployment_name}.cloud.databricks.com"


# workspace filter ends here

        url = f"{workspace_instance}/api/2.0/sql/history/queries"
        headers = {
            "Authorization": f"Bearer {get_dbx_access_token()}",
            "Content-Type": "application/json",
        }
        now_ms = int(time.time() * 1000)
        # NEW: Calculate minimum duration threshold in seconds - Query Data
        min_duration_sec = (hours * 3600) + (minutes * 60) + seconds
        # print("now_ms :", now_ms)
        params = {
            "max_results": 100,
            # "warehouse_id": warehouse_id
        }

        r = requests.get(url, headers=headers, params=params)
        # data = resp.json()
        # print("STATUS CODE:", r.status_code)
        # print("RAW RESPONSE:", data["res"])
        r.raise_for_status()
        data = r.json()

        # print("data222222222222222222222222222222222222222222222222222",data)
        # print(data["has_next_page"], data.get("next_page_token"))
        # for q in data["res"]:
        #     print(q["query_id"], q["status"], q.get("query_text", "")[:60])

        # resp.raise_for_status()
        print("FULL RESPONSE:", data)
        queries = data.get("res", [])
        # print("TOTAL QUERIES FROM DBX:", queries)
        # print("resp", r)
        print("queries: ", queries[0].get("user_name"))
        results = []

        for q in queries:
            status = q.get("status")
            user_name = q.get("user_name") or ""
            # NEW: Get warehouse ID for filtering - Query Data
            query_warehouse = q.get("warehouse_id")

            # print("usernaeeeeeeeeeeeeeeeeeeee",user_name )

            if status_filter and status != status_filter:
                continue

            # if user_filter and user_name != user_filter:
            #     continue
            # NEW: Filter by warehouse if specified - Query Data
            if warehouse_id and warehouse_id != query_warehouse:
                continue

            # NEW: Partial match user filter (case-insensitive substring search) - Query Data
            if user_filter:
                if user_filter.strip().lower() not in user_name.lower():
                    continue

            # existing duration logic below

            start = q.get("query_start_time_ms")
            end = q.get("query_end_time_ms") or now_ms
            # print(f"start is {start} and end is {end}")
            if not start:
                continue

            duration_sec = round((end - start) / 1000, 2)
            # NEW: Filter by minimum duration threshold - Query Data
            if min_duration_sec > 0 and duration_sec < min_duration_sec:
                continue

            # if since_minutes:
            #     cutoff_ms = now_ms - (since_minutes * 60 * 1000)
            #     print("cutoff_ms: ", cutoff_ms)
            #     if start < cutoff_ms:
            #         continue

            # duration_sec = round((end - start) / 1000, 2)
            # print("duration_sec: ", duration_sec)
            # print("append ke upar")
            results.append(
                {
                    "query_id": q.get("query_id"),
                    "status": status,
                    "start_time": start,
                    "duration": duration_sec,
                    "user": user_name,
                    # NEW: Include full query text for viewing/optimization - Query Data
                    "query_text": q.get("query_text"),
                }
            )

        # print("RESULTSSSSSS_before sortin", results)

        results.sort(key=lambda x: x["duration"], reverse=True)
        # print("RESULTSSSSSS", results)
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ================= NEW FUNCTIONS BELOW =================


# NEW: Send email notification when a query is killed
def send_kill_email(
    to_email: str,
    query_id: str,
    original_query: str = None,
    optimized_query: str = None,
) -> dict:

    try:
        if not to_email:
            raise ValueError("Recipient email is required")
        if not query_id:
            raise ValueError("query_id is required")

        user_name = to_email.split("@")[0].replace(".", " ").title()
        current_time = datetime.now().strftime("%d-%b-%Y %I:%M %p")

        subject = "Databricks Query Terminated – Optimization Suggested"

        # -------- Plain Text Fallback --------
        text_body = f"""
Dear {user_name},

Your query has been terminated.

Date: {current_time}
User: {to_email}
Query ID: {query_id}
Status: KILLED

Current Query:
{original_query or "Not available"}

AI Optimized Query:
{optimized_query or "Optimization not available"}

For more info:
https://www.databricks.com/discover/pages/optimize-data-workloads-guide#auto-optimize
https://learn.microsoft.com/en-us/azure/databricks/optimizations/

Thanks,
Lakehouse Team
""".strip()

        # -------- HTML Version --------
        html_body = f"""
<html>
  <body style="font-family: Arial, Helvetica, sans-serif; font-size: 15px; color: #1f2933; line-height: 1.6;">

    <!-- Greeting -->
    <p style="margin-bottom: 18px;">
      Dear <strong>{user_name}</strong>,
    </p>

    <p style="margin-bottom: 16px; font-size: 14px">
      Your Databricks SQL query has been terminated using the dashboard action.
    </p>

    <!-- Summary Table -->
    <table style="border-collapse: collapse; width: 100%; font-size: 14px; margin-bottom: 22px;" border="1" cellpadding="8">
      <tr style="background-color: #f4f6f8;">
        <th>Date</th>
        <th>User</th>
        <th>Query ID</th>
        <th>Status</th>
      </tr>
      <tr>
        <td>{datetime.now().strftime("%d-%b-%Y %I:%M %p")}</td>
        <td>{to_email}</td>
        <td>{query_id}</td>
        <td style="color: red; font-weight: bold;">KILLED</td>
      </tr>
    </table>

    <!-- AI Disclaimer -->
    <p style="font-size: 15px; margin-bottom: 8px;">
      <strong>Note:</strong>
    </p>

    <p style="background-color: #f8fafc; padding: 10px; border-left: 4px solid #2563eb; margin-bottom: 24px; font-size:15px">
      The optimized query provided below is an AI-generated recommendation.
      Please carefully review, validate, and test it before implementing it in your workflows.
    </p>

    <!-- Side-by-side Queries -->
<table style="width:100%; border-collapse: collapse; margin-top: 10px;" cellpadding="10">
  <tr>
   <!-- Current Query -->
<div style="width: 48%; display: inline-block; vertical-align: top;">
  <div style="font-weight: bold; margin-bottom: 6px; font-size: 14px;">
    Current Query
  </div>
  <div style="
      background-color: #fdf2f2;
      border-left: 5px solid #d32f2f;
      padding: 12px;
      height: 220px;
      overflow: auto;
      font-family: Consolas, monospace;
      font-size: 13px;
      white-space: pre;
  ">
    {original_query or "Not available"}
  </div>
</div>

<!-- Optimized Query -->
<div style="width: 48%; display: inline-block; vertical-align: top; margin-left: 3%;">
  <div style="font-weight: bold; margin-bottom: 6px; font-size: 14px;">
    AI Optimized Query
  </div>
  <div style="
      background-color: #eef6ff;
      border-left: 5px solid #2563eb;
      padding: 12px;
      height: 220px;
      overflow: auto;
      font-family: Consolas, monospace;
      font-size: 13px;
      white-space: pre;
  ">
    {optimized_query or "Optimization not available"}
  </div>
</div>

  </tr>
</table>


    <!-- Resources -->
    <p style="margin-top: 24px;">
      For more information on query optimization:
    </p>

    <ul>
      <li>
        <a href="https://www.databricks.com/discover/pages/optimize-data-workloads-guide#auto-optimize">
          Databricks Optimize Guide
        </a>
      </li>
      <li>
        <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/">
          Azure Databricks Optimization Docs
        </a>
      </li>
    </ul>

    <!-- Closing -->
    <p style="margin-top: 28px;">
      Regards,<br>
      <strong>Lakehouse Team</strong>
    </p>

    <hr style="margin-top: 30px;">

    <p style="font-size: 12px; color: #6b7280;">
      Confidentiality Notice: This email contains information intended only for the recipient.
      If received in error, please notify the sender and delete it immediately.
    </p>

  </body>
</html>
"""

        msg = Message(
            subject=subject,
            recipients=[to_email],
            body=text_body,
            html=html_body,
        )

        mail.send(msg)

        print(f"[MAIL] Optimization email sent to {to_email} for query {query_id}")
        return {"status": "success", "message": f"Email sent to {to_email}"}

    except Exception as e:
        print("[MAIL] Failed to send kill email:", str(e))
        return {"status": "failed", "message": str(e)}


def get_optimized_query_from_ai(original_query: str) -> str:
    print("[AI] Optimizing query")

    if not original_query:
        return "No query text available for optimization."

    prompt = f"""
You are a senior data engineer.
Optimize the following Databricks SQL query for performance.

Rules:
- Return ONLY the optimized SQL query
- No explanations
- Assume the query is slow and optimize aggressively

QUERY:
{original_query}
"""

    try:
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.1,
                "max_output_tokens": 1024,
            },
        )
        optimized_query = response.text.strip()
        print(response)
        print("\n========== GEMINI SQL OPTIMIZATION ==========")
        print("----- ORIGINAL QUERY -----")
        print(original_query)
        print("\n----- OPTIMIZED QUERY -----")
        print(optimized_query)
        print("===========================================\n")

        return optimized_query

    except Exception as e:
        print(f"[WARN] AI optimization failed: {e}")
        return "AI optimization failed."


# NEW: API endpoint to list all SQL warehouses available in the workspace - Query Data
@app.route("/api/sql/warehouses", methods=["GET"])
def list_sql_warehouses():
    """
    Fetch and return list of SQL warehouses from Databricks.
    Used to populate warehouse dropdown in UI.
    """
    try:
        # print("111")
        url = f"{WORKSPACE_INSTANCE}/api/2.0/sql/warehouses"
        headers = {
            "Authorization": f"Bearer {get_dbx_access_token()}",
            "Content-Type": "application/json",
        }

        r = requests.get(url, headers=headers)
        # print("r ", r)
        r.raise_for_status()

        data = r.json()
        # print("data",data)
        warehouses = data.get("warehouses", [])

        # NEW: Return simplified list with only warehouse IDs - Query Data

        #  this chnage is for change of warehouse name instead of warehouse id 
        return jsonify([
    {
        "id": w.get("id"),
        "name": w.get("name")
    }
    for w in warehouses
])

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)

# end of the file
