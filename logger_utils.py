import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any

from databricks import sql

# Import necessary exceptions from the Databricks SQL Connector
from databricks.sql.exc import DatabaseError, ProgrammingError, OperationalError

# Configuration
AUDIT_MODE = os.getenv("AUDIT_MODE", "delta")  # "delta" or "file"
AUDIT_FILE_PATH = os.getenv("AUDIT_LOG_PATH", "./logs/audit_log.csv")
AUDIT_DELTA_TABLE = os.getenv("AUDIT_DELTA_TABLE", "tata_motors.deep.dpm_audit_log")

# Request-scoped context for audit
_AUDIT_CONTEXT = {
    "run_id": None,
    "admin": None,
    "file_path": None,
    "row_id": None,
    "request_payload": None,
    "response_code": None,
    "response_body": None,
}

EXPECTED_COLUMNS = [
    "run_id",
    "ts",
    "admin",
    "file_path",
    "row_id",
    "action",
    "principal_type",
    "principal_identifier",
    "status",
    "details",
    "request_payload",
    "response_code",
    "response_body",
]

def _is_databricks_env() -> bool:
    """Detect if running in Databricks environment by presence of key env vars."""
    return bool(os.getenv("WORKSPACE_INSTANCE") and os.getenv("WORKSPACE_TOKEN") and os.getenv("DATABRICKS_HTTP_PATH"))

# def get_sql_connection():
#     """Create and return a new Databricks SQL connector client."""
#     return sql.connect(
#         server_hostname=os.getenv("WORKSPACE_INSTANCE"),
#         http_path=os.getenv("DATABRICKS_HTTP_PATH"),
#         access_token=os.getenv("WORKSPACE_TOKEN"),
#     )

def get_sql_connection():
    """Create and return a new Databricks SQL connector client, with robust error handling."""
    
    hostname = os.getenv("WORKSPACE_INSTANCE")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    # This is the key variable for the token, injected via secret scope
    access_token = os.getenv("WORKSPACE_TOKEN") 

    # Truncate values for safe logging
    host_check = hostname[:20] if hostname else 'NONE'
    http_check = http_path[:10] if http_path else 'NONE'

    print(f"ATTEMPTING DB CONNECTION. Host: {host_check}..., HTTP Path: {http_check}..., Token Status: {'PRESENT' if access_token else 'MISSING'}")

    try:
        # Check 1: Ensure critical environment variables are actually present
        if not hostname or not http_path or not access_token:
             # Raise a clear error if any config is missing
             raise ValueError("Required Databricks connection parameters (Host, HTTP Path, or Token) are MISSING in the environment.")
             
        # Check 2: Attempt the connection with a short timeout
        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=access_token,
            # CRITICAL: Set a client-side timeout to fail quickly (~10 seconds)
            connection_timeout=10 
        )
    # Catch specific exceptions from the databricks-sql-connector
    except (DatabaseError, ProgrammingError, TimeoutError, OperationalError, AuthenticationError, ValueError) as e:
        # Log the specific error and re-raise to see the root cause immediately
        print(f"CRITICAL DATABRICKS CONNECTION FAILURE: {type(e).__name__} - {e}")
        raise 
    except Exception as e:
        print(f"CRITICAL UNEXPECTED ERROR during DB connect: {type(e).__name__} - {e}")
        raise


# def get_sql_connection():
#     """Create and return a new Databricks SQL connector client, with robust error handling."""
    
#     hostname = os.getenv("WORKSPACE_INSTANCE")
#     http_path = os.getenv("DATABRICKS_HTTP_PATH")
#     # This is the key variable for the token, injected via secret scope
#     access_token = os.getenv("WORKSPACE_TOKEN") 

#     # Truncate values for safe logging
#     host_check = hostname[:20] if hostname else 'NONE'
#     http_check = http_path[:10] if http_path else 'NONE'
#     print("DEBUG url:", url)
#     print("DEBUG auth header:", HEADERS_WS.get("Authorization","")[:10] + "...")
#     Print("hostnameL",hostname)
#     print("http_pathL",http_path)
#     print("access_tokenL",access_token)


#     print(f"ATTEMPTING DB CONNECTION. Host: {host_check}..., HTTP Path: {http_check}..., Token Status: {'PRESENT' if access_token else 'MISSING'}")

#     try:
#         # Check 1: Ensure critical environment variables are actually present
#         if not hostname or not http_path or not access_token:
#              # Raise a clear error if any config is missing
#              raise ValueError("Required Databricks connection parameters (Host, HTTP Path, or Token) are MISSING in the environment.")
             
#         # Check 2: Attempt the connection with a short timeout
#         return sql.connect(
#             server_hostname=hostname,
#             http_path=http_path,
#             access_token=access_token,
#             # CRITICAL: Set a client-side timeout to fail quickly (~10 seconds)
#             connection_timeout=10 
#         )
#     # Catch specific exceptions from the databricks-sql-connector
#     except (DatabaseError, ProgrammingError, TimeoutError, OperationalError, AuthenticationError, ValueError) as e:
#         # Log the specific error and re-raise to see the root cause immediately
#         print(f"CRITICAL DATABRICKS CONNECTION FAILURE: {type(e).__name__} - {e}")
#         raise 
#     except Exception as e:
#         print(f"CRITICAL UNEXPECTED ERROR during DB connect: {type(e).__name__} - {e}")
#         raise

def execute(sql_text: str, params: Optional[dict] = None):
    """Execute a SQL command (non-select) with optional parameters."""
    with get_sql_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params or {})

def ensure_audit_table_sql():
    """Create audit Delta table if it doesn't exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_DELTA_TABLE} (
      run_id STRING,
      ts TIMESTAMP,
      admin STRING,
      file_path STRING,
      row_id STRING,
      action STRING,
      principal_type STRING,
      principal_identifier STRING,
      status STRING,
      details STRING,
      request_payload STRING,
      response_code STRING,
      response_body STRING
    )
    USING DELTA
    """
    ddl2=f"GRANT ALL PRIVILEGES ON TABLE {AUDIT_DELTA_TABLE} TO `account users`"
    
    try:
        execute(ddl)
        execute(ddl2)
    except (DatabaseError, OperationalError, AuthenticationError, Exception) as e:
        print(f"[ERROR] Could not create audit table: {e}")

def insert_audit_sql(row: Dict[str, Any]):
    """Insert one audit row into the Delta audit table."""
    cols = ", ".join(row.keys())
    placeholders = ", ".join([f":{k}" for k in row.keys()])
    sql_text = f"INSERT INTO {AUDIT_DELTA_TABLE} ({cols}) VALUES ({placeholders})"
    execute(sql_text, row)

def _append_to_file(row: Dict[str, Any]) -> bool:
    """Append audit entry to a local CSV file (fallback)."""
    try:
        audit_path = AUDIT_FILE_PATH
        parent = os.path.dirname(audit_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # If file doesn't exist, write header; else append
        header = not os.path.isfile(audit_path)
        df = pd.DataFrame([row])
        df.to_csv(audit_path, mode="a", header=header, index=False)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write audit log file: {e}")
        return False

def append_audit(
    action: str,
    principal: str,
    identifier: str,
    status: str,
    details: str = "",
    response_code: Optional[int] = None,
    response_body: Optional[Any] = None,
    request_payload: Optional[Any] = None,
):
    """Add an audit log entry, via Delta table or file fallback."""
    ts_utc = datetime.utcnow().isoformat() + "Z"
    ctx = dict(_AUDIT_CONTEXT)
    request_payload = request_payload if request_payload is not None else ctx.get("request_payload")
    response_code = response_code if response_code is not None else ctx.get("response_code")
    response_body = response_body if response_body is not None else ctx.get("response_body")

    row = {
        "run_id": ctx.get("run_id") or "",
        "ts": ts_utc,
        "admin": ctx.get("admin") or "",
        "file_path": ctx.get("file_path") or "",
        "row_id": str(ctx.get("row_id")) if ctx.get("row_id") is not None else "",
        "action": action,
        "principal_type": principal,
        "principal_identifier": identifier,
        "status": status,
        "details": details or "",
        "request_payload": json.dumps(request_payload) if request_payload is not None else "",
        "response_code": str(response_code) if response_code is not None else "",
        "response_body": json.dumps(response_body) if response_body is not None else "",
    }

    if AUDIT_MODE == "delta" and _is_databricks_env():
        try:
            ensure_audit_table_sql()
            insert_audit_sql(row)
        except Exception as e:
            print(f"[WARN] SQL insert failed: {e}. Falling back to file.")
            _append_to_file(row)
    else:
        _append_to_file(row)

def read_audit(limit: int = 1000, filters: Optional[Dict[str, Any]] = None):
    """Read audit log entries from Delta table or file fallback."""
    if AUDIT_MODE == "delta" and _is_databricks_env():
        try:
            sql_text = f"SELECT * FROM {AUDIT_DELTA_TABLE}"
            if filters:
                conds = []
                for k, v in filters.items():
                    # Simple string equality filter; escape single quotes
                    v_escaped = str(v).replace("'", "''")
                    conds.append(f"{k} = '{v_escaped}'")
                if conds:
                    sql_text += " WHERE " + " AND ".join(conds)
            sql_text += f" ORDER BY ts DESC LIMIT {limit}"
            with get_sql_connection().cursor() as cur:
                cur.execute(sql_text)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                df = pd.DataFrame(rows, columns=columns)
                return df
        except Exception as e:
            print(f"[WARN] SQL read failed: {e}. Falling back to file.")

    # Fallback to reading from file
    try:
        df = pd.read_csv(AUDIT_FILE_PATH)
        if filters:
            for k, v in filters.items():
                if k in df.columns:
                    df = df[df[k] == v]
        if "ts" in df.columns:
            df = df.sort_values("ts", ascending=False)
        return df.head(limit)
    except Exception as e:
        print(f"[ERROR] Failed to read audit log file: {e}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

def set_audit_context(**kwargs):
    """Set request-scoped audit context variables."""
    global _AUDIT_CONTEXT
    for k, v in kwargs.items():
        if k in _AUDIT_CONTEXT:
            _AUDIT_CONTEXT[k] = v

def clear_audit_context():
    """Clear the request-scoped audit context."""
    global _AUDIT_CONTEXT
    for k in _AUDIT_CONTEXT.keys():
        _AUDIT_CONTEXT[k] = None