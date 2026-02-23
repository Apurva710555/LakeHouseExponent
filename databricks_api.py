"""Databricks REST API helper for SCIM and Jobs endpoints (robust version)."""

import os
import time
import requests
from typing import Any, Dict, Optional, Union
from dbx_auth import get_dbx_access_token


# ---------------------------------------------------------------------
# Environment Setup
# ---------------------------------------------------------------------
INSTANCE = os.getenv(
    "DATABRICKS_INSTANCE"
)  # Base URL; typically accounts host when using account-level APIs
WORKSPACE_INSTANCE = os.getenv(
    "WORKSPACE_INSTANCE"
)  # Optional: workspace base URL for workspace-level SCIM fallback
TOKEN = os.getenv("DATABRICKS_TOKEN")  # Account-level token (or workspace if re-used)
WORKSPACE_TOKEN = (
    os.getenv("WORKSPACE_TOKEN") or TOKEN
)  # Prefer dedicated workspace token
ACCOUNT = os.getenv("ACCOUNT_ID")

# Do not raise at import; allow workspace-only usage (e.g., /Me) without account config
if not WORKSPACE_INSTANCE and not INSTANCE:
    print("[WARN] Neither WORKSPACE_INSTANCE nor DATABRICKS_INSTANCE is set.")

HEADERS_ACCOUNT = {
    "Authorization": f"Bearer {TOKEN}" if TOKEN else "",
    "Content-Type": "application/json",
}
HEADERS_WS = {
    "Authorization": f"Bearer {WORKSPACE_TOKEN}" if WORKSPACE_TOKEN else "",
    "Content-Type": "application/json",
}


def _auth_headers():
    return {
        "Authorization": f"Bearer {get_dbx_access_token()}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------
# Internal Helper
# ---------------------------------------------------------------------
# Shorter defaults for UI-backed flows (like project onboarding)
FAST_TIMEOUT_SECS = 12
FAST_RETRIES = 2


def _url(path: str) -> str:
    """Return full Databricks API URL."""
    return INSTANCE.rstrip("/") + path


def _ws_url(path: str) -> Optional[str]:
    """Return workspace API URL if WORKSPACE_INSTANCE is configured."""
    base = WORKSPACE_INSTANCE or INSTANCE
    if not base:
        return None
    return base.rstrip("/") + path


def _safe_request(
    method: str,
    url: str,
    retries: int = 3,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    **kwargs,
) -> Dict[str, Any]:
    """Perform a safe HTTP request with retries and consistent return shape."""
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method,
                url,
                headers=(headers or _auth_headers()),
                timeout=timeout,
                **kwargs,
            )
            # --- Success path ---
            if resp.status_code in (200, 201):
                try:
                    return {"status_code": resp.status_code, "body": resp.json()}
                except Exception:
                    return {
                        "status_code": resp.status_code,
                        "body": {},
                        "message": "Empty body",
                    }
            elif resp.status_code == 204:
                # No Content (normal for SCIM PATCH/DELETE)
                return {"status_code": 204, "body": {}, "message": "No content"}
            else:
                # --- API returned an error ---
                try:
                    body = resp.json()
                except Exception:
                    body = {"error": resp.text}
                return {
                    "status_code": resp.status_code,
                    "body": body,
                    "error": f"{method} {url} failed: {resp.status_code}",
                }

        except requests.exceptions.RequestException as e:
            # --- Network/connection issues ---
            if attempt == retries:
                return {
                    "status_code": 0,
                    "body": {},
                    "error": f"{method} {url} network error after {retries} attempts: {e}",
                }
            time.sleep(delay)
            delay *= 2  # exponential backoff


# ---------------------------------------------------------------------
# SCIM: USERS
# ---------------------------------------------------------------------
def scim_list_users(filter_: Optional[str] = None) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Users")
    params = {"filter": filter_} if filter_ else None
    return _safe_request(
        "GET", url, params=params, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def scim_me_workspace() -> Dict[str, Any]:
    """
    Get current workspace user using SCIM Me endpoint. Requires WORKSPACE_INSTANCE and DATABRICKS_TOKEN.
    Returns dict with keys: status_code, body, and possibly error.
    """
    url = _ws_url("/api/2.0/preview/scim/v2/Me")
    if not url:
        return {"status_code": 0, "body": {}, "error": "WORKSPACE_INSTANCE not set"}
    return _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )


def scim_create_user(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Users")
    return _safe_request(
        "POST", url, json=payload, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def scim_patch_user(user_id: str, patch_ops: Dict[str, Any]) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Users/{user_id}")
    return _safe_request(
        "PATCH", url, json=patch_ops, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def scim_delete_user(user_id: str) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Users/{user_id}")
    return _safe_request("DELETE", url, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES)


# ---------------------------------------------------------------------
# SCIM: GROUPS
# ---------------------------------------------------------------------
def scim_list_groups(filter_: Optional[str] = None) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Groups")
    params = {"filter": filter_} if filter_ else None
    return _safe_request(
        "GET", url, params=params, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def scim_create_group(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Groups")
    return _safe_request(
        "POST", url, json=payload, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def scim_patch_group(group_id: str, patch_ops: Dict[str, Any]) -> Dict[str, Any]:
    """Handles 204 No Content gracefully."""
    url = _url(f"/api/2.1/accounts/{ACCOUNT}/scim/v2/Groups/{group_id}")
    return _safe_request(
        "PATCH", url, json=patch_ops, timeout=FAST_TIMEOUT_SECS, retries=FAST_RETRIES
    )


def create_project_folder(application_name):
    """Creates a folder structure in the Databricks workspace for the new project."""
    base_path = f"/Workspace/{application_name}"
    url = _ws_url(f"/api/2.0/workspace/mkdirs")
    return _safe_request(
        "POST",
        url,
        headers=_auth_headers(),
        json={"path": base_path},
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    ), base_path


def build_group_name(organization_name, department, application_name, environment):
    """Builds 3 groups for the project based on the input parameters."""
    # Determine the prefix based on the environment
    prefix = "nprod" if environment.lower() == "dev" else "prod"

    base_name = f"{organization_name}_{department}_{application_name}"
    ## nprod_TMCV_Sales_SCV_CoPilot
    group_roles = ["administrators", "contributors", "readers"]

    # Construct the full group names
    group_names = [f"{prefix}_{base_name}_{role}" for role in group_roles]
    return group_names


def get_policy_id(policy_name):
    """Retrieves a policy ID given its name."""
    url = _ws_url(f"/api/2.0/policies/clusters/list")
    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )
    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code != 200:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(
            f"Failed to retrieve cluster policies list. API Error: {error_msg}"
        )

    policy_list = response_body.get("policies", [])
    found_id = None
    for policy in policy_list:
        if policy.get("name") == policy_name:
            found_id = policy.get("policy_id")
            break
    if found_id:
        return found_id
    else:
        raise RuntimeError(
            f"Cluster policy '{policy_name}' not found in the workspace."
        )


def get_cluster_id(cluster_name):
    """Retrieves the ID of a cluster given its name, if it exists."""
    url = _ws_url(f"/api/2.0/clusters/list")
    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code != 200:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(f"Failed to retrieve cluster list. API Error: {error_msg}")
    cluster_list = response_body.get("clusters", [])

    found_id = None
    for cluster in cluster_list:
        if cluster.get("cluster_name") == cluster_name:
            found_id = cluster.get("cluster_id")
            break
    return found_id


def create_all_purpose_cluster(
    application_name, policy_id, node_type_id, base_workers, environment, project_tags
):
    """Provisions a new all-purpose cluster for the project using the specified parameters."""
    cluster_name = f"{application_name}"

    cluster_id = get_cluster_id(cluster_name)
    if cluster_id:
        print(
            f"All-purpose cluster '{cluster_name}' already exists. Using existing ID: {cluster_id}"
        )
        return cluster_id

    spark_version = "16.4.x-scala2.12"
    base_workers_int = int(base_workers)

    payload = {
        "cluster_name": cluster_name,
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "driver_node_type_id": node_type_id,
        "autotermination_minutes": 10,
        "autoscale": {
            "min_workers": base_workers_int,
            "max_workers": 2 * base_workers_int,
        },
        "spark_env_vars": {"ENV_NAME": environment},
        "custom_tags": project_tags,
        "data_security_mode": "USER_ISOLATION",
        "policy_id": policy_id,
    }
    print(f"Creating all-purpose cluster '{cluster_name}'...")

    url = _ws_url(f"/api/2.0/clusters/create")
    api_response = _safe_request(
        "POST",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    if api_response.get("status_code") == 200:
        new_cluster_id = api_response.get("body", {}).get("cluster_id")

        if new_cluster_id:
            return new_cluster_id
        else:
            raise RuntimeError(
                f"Cluster creation succeeded but ID was missing from response body: {api_response.get('body')}"
            )
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to create cluster '{cluster_name}'. API Error: {error_msg}"
        )


def get_sql_warehouse_id(wh_name):
    """Retrieves the ID of a SQL warehouse given its name, if it exists."""
    url = _ws_url(f"/api/2.0/sql/warehouses")
    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )
    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code != 200:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(f"Failed to retrieve warehouse list. API Error: {error_msg}")
    wh_list = response_body.get("warehouses", [])

    found_id = None
    for wh in wh_list:
        if wh.get("name") == wh_name:
            found_id = wh.get("id")
            break
    return found_id


def create_sql_warehouse(application_name, sql_wh_size, project_tags):
    """Creates a SQL warehouse for the project."""
    wh_name = f"{application_name}_Reporting_wh"

    wh_id = get_sql_warehouse_id(wh_name)
    if wh_id:
        print(f"SQL warehouse '{wh_name}' already exists. Using existing ID: {wh_id}")
        return wh_id

    custom_tags_list = [{"key": k, "value": v} for k, v in project_tags.items() if v]
    formatted_tags = {"custom_tags": custom_tags_list}

    payload = {
        "name": wh_name,
        "cluster_size": sql_wh_size,
        "auto_stop_mins": 10,
        "min_num_clusters": 1,
        "max_num_clusters": 2,
        "enable_serverless_compute": True,
        "tags": formatted_tags,
    }
    print(f"Creating SQL warehouse '{wh_name}'...")

    url = _ws_url(f"/api/2.0/sql/warehouses")
    api_response = _safe_request(
        "POST",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    if api_response.get("status_code") == 200:
        new_wh_id = api_response.get("body", {}).get("id")

        if new_wh_id:
            return new_wh_id
        else:
            raise RuntimeError(
                f"SQL Warehouse creation succeeded but ID was missing from response body: {api_response.get('body')}"
            )
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to create SQL Warehouse '{wh_name}'. API Error: {error_msg}"
        )


def get_object_id(object_name):
    """Retrives the Folder id"""
    url = _ws_url(f"/api/2.0/workspace/get-status")
    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        params={"path": f"{object_name}"},
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code == 200:
        found_id = response_body.get("object_id")
        if found_id:
            return found_id
        else:
            raise RuntimeError(
                f"'object_id' was missing from response body for path: {object_name}"
            )
    else:
        error_msg = api_response.get("error")
        if status_code == 404:
            raise RuntimeError(
                f"Workspace object not found at path '{object_name}' (Status 404)."
            )
        else:
            raise RuntimeError(
                f"Failed to get object status for path '{object_name}'. API Error: {error_msg}"
            )


def get_group_id(group_name):
    """Retrieves Group ID"""
    url = _ws_url(f"/api/2.0/preview/scim/v2/Groups")

    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        params={"filter": f'displayName eq "{group_name}"'},
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code == 200:
        # Success: The response body contains a list of resources (Groups)
        resources = response_body.get("Resources", [])
        if resources:
            return resources[0].get("id")
        else:
            return None
    else:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(f"Failed to retrieve Group ID. API Error: {error_msg}")


def create_project_groups(group_full_name):
    """Checks for and creates a project group using the SCIM API."""
    group_id = get_group_id(group_full_name)
    if group_id:
        print(
            f"Group '{group_full_name}' already exists. Using existing ID: {group_id}"
        )
        return group_id

    url = _ws_url(f"/api/2.0/preview/scim/v2/Groups")
    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "displayName": group_full_name,
    }

    api_response = _safe_request(
        "POST",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code in (200, 201):
        # Group created successfully
        new_group_id = response_body.get("id")
        if new_group_id:
            return new_group_id
        else:
            raise RuntimeError(
                f"Group creation succeeded but ID was missing for {group_full_name}: {response_body}"
            )
    else:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(
            f"Failed to create group '{group_full_name}'. API Error: {error_msg}"
        )


def set_all_folder_permissions(folder_path, acl_list):
    """Sets all specified permissions for a folder in a single PUT request."""
    request_obj_id = get_object_id(folder_path)

    if not request_obj_id:
        print(
            f"Error: Could not retrieve object ID for folder '{folder_path}'. Permissions skipped."
        )
        return

    url = _ws_url(f"/api/2.0/permissions/directories/{request_obj_id}")
    payload = {"access_control_list": acl_list}

    print(f"Setting ALL folder permissions for '{folder_path}'...")
    api_response = _safe_request(
        "PUT",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")

    if status_code == 200:
        print(f"SUCCESS: Folder permissions set for '{folder_path}'.")
        return
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to set folder permissions for '{folder_path}'. API Error: {error_msg}"
        )


def set_all_cluster_permissions(cluster_id, acl_list):
    """Sets the complete set of permissions for a cluster using a single PUT request."""
    if not cluster_id:
        print("Error: Cluster ID is missing. Cluster permissions skipped.")
        return

    url = _ws_url(f"/api/2.0/permissions/clusters/{cluster_id}")
    payload = {"access_control_list": acl_list}

    print(f"Setting ALL permissions on cluster '{cluster_id}'...")
    api_response = _safe_request(
        "PUT",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    if status_code == 200:
        print(f"SUCCESS: Cluster permissions set for '{cluster_id}'.")
        return
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to set cluster permissions for '{cluster_id}'. API Error: {error_msg}"
        )


def set_all_sql_warehouse_permissions(warehouse_id, acl_list):
    """Sets the complete set of permissions for a SQL warehouse using a single PUT request."""
    if not warehouse_id:
        print("Error: SQL Warehouse ID is missing. Warehouse permissions skipped.")
        return

    url = _ws_url(f"/api/2.0/permissions/sql/warehouses/{warehouse_id}")
    payload = {"access_control_list": acl_list}

    print(f"Setting ALL permissions on SQL warehouse '{warehouse_id}'...")
    api_response = _safe_request(
        "PUT",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    if status_code == 200:
        print(f"SUCCESS: SQL Warehouse permissions set for '{warehouse_id}'.")
        return
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to set sql warehouse permissions for '{warehouse_id}'. API Error: {error_msg}"
        )


def get_user_id(user_email):
    """Retrieves the unique SCIM User ID given the user's email address"""
    url = _ws_url(f"/api/2.0/preview/scim/v2/Users")
    params = {"filter": f'userName eq "{user_email}"'}

    api_response = _safe_request(
        "GET",
        url,
        headers=_auth_headers(),
        params=params,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})
    if status_code == 200:
        resources = response_body.get("Resources", [])
        if resources:
            return resources[0].get("id")
    else:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(
            f"Failed to retrieve User ID for '{user_email}'. API Error: {error_msg}"
        )


def add_user_to_group_by_id(group_id, group_full_name, user_email):
    """Adds a user to the project group using the group's ID."""
    user_id_to_add = get_user_id(user_email)
    print(f"User ID to add: {user_id_to_add}")
    if not user_id_to_add:
        print(
            f"Cannot add user. User with email '{user_email}' not found in Databricks workspace."
        )
        return False

    url = _ws_url(f"/api/2.0/preview/scim/v2/Groups/{group_id}")
    payload = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
                "op": "add",
                "path": "members",
                "value": [{"value": user_id_to_add, "type": "User"}],
            }
        ],
    }

    print(f"Adding owner '{user_email}' to group '{group_full_name}'...")

    api_response = _safe_request(
        "PATCH",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    if status_code in (200, 204):
        print(f"SUCCESS: User '{user_email}' added to group '{group_full_name}'.")
        return True
    else:
        error_msg = api_response.get("error") or str(api_response.get("body"))
        raise RuntimeError(
            f"Failed to add user '{user_email}' to group '{group_full_name}'. API Error: {error_msg}"
        )


def create_project_schema(
    catalog_name: str, schema_name: str, storage_root_url: str
) -> Dict[str, Any]:
    """
    Creates a Unity Catalog SCHEMA with a specified storage root (external location).
    Requires the full catalog name (e.g., 'catalog_a') and the schema name.
    """
    if not all([catalog_name, schema_name, storage_root_url]):
        raise ValueError(
            "Catalog name, schema name, and storage root URL are required for schema creation."
        )

    full_schema_name = f"{catalog_name}.{schema_name}"
    url = _ws_url("/api/2.1/unity-catalog/schemas")

    payload = {
        "name": schema_name,
        "catalog_name": catalog_name,
        "storage_root": storage_root_url,
        "comment": f"Schema for project {schema_name} in catalog {catalog_name}",
    }

    print(
        f"Creating Unity Catalog Schema: '{full_schema_name}' with storage root: {storage_root_url}..."
    )

    # Use POST method for schema creation
    api_response = _safe_request(
        "POST",
        url,
        headers=_auth_headers(),
        json=payload,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )

    status_code = api_response.get("status_code")
    response_body = api_response.get("body", {})

    if status_code in (200, 201):
        print(f"SUCCESS: Unity Catalog Schema '{full_schema_name}' created.")
        return response_body

    # Handle the "already exists" case gracefully (Status code 409 Conflict)
    elif status_code == 409:
        print(
            f"WARNING: Unity Catalog Schema '{full_schema_name}' already exists. Skipping creation."
        )
        return {"name": schema_name, "catalog_name": catalog_name, "status": "exists"}

    else:
        error_msg = api_response.get("error") or str(response_body)
        raise RuntimeError(
            f"Failed to create Unity Catalog Schema '{full_schema_name}'. API Error: {error_msg}"
        )


def get_dashboards_list():
    """Retrieves the list of all Lakeview dashboards in the workspace."""
    url = _ws_url("/api/2.0/lakeview/dashboards")
    all_dashboards = []
    page_token = None

    while True:
        params = {"page_size": 1000}
        if page_token:
            params["page_token"] = page_token

        api_response = _safe_request(
            "GET",
            url,
            params=params,
            headers=_auth_headers(),
            timeout=FAST_TIMEOUT_SECS,
            retries=FAST_RETRIES,
        )

        status_code = api_response.get("status_code")
        response_body = api_response.get("body", {})

        if status_code != 200:
            error_msg = api_response.get("error") or str(response_body)
            raise RuntimeError(
                f"Failed to retrieve dashboards list. API Error: {error_msg}"
            )

        # Extract dashboards
        all_dashboards.extend(response_body.get("dashboards", []))

        # Correct! Pagination token is in response body
        page_token = response_body.get("next_page_token")

        if not page_token:
            break

    # Build final list
    return [
        {"name": d.get("display_name"), "id": d.get("dashboard_id")}
        for d in all_dashboards
    ]


# NEW Function to cancel the query , this calls the databricks API and returns response - Query Data
def cancel_sql_query(query_id: str, user: str) -> None:
    """
    Cancels a running Databricks SQL query by query_id.

    Raises:
        RuntimeError if the API call fails
    """
    print("Inside cancel_sql_query")

    headers = {
        "Authorization": f"Bearer {get_dbx_access_token()}",
        "Content-Type": "application/json",
    }

    if not query_id:
        raise ValueError("query_id is required to cancel SQL query")

    url = _ws_url(f"/api/2.0/sql/statements/{query_id}/cancel")

    if not url:
        raise RuntimeError("WORKSPACE_INSTANCE is not configured")
    print("url: " + url)

    api_response = _safe_request(
        method="POST",
        url=url,
        headers=headers,
        timeout=FAST_TIMEOUT_SECS,
        retries=FAST_RETRIES,
    )
    print("api response: ", api_response)
    status_code = api_response.get("status_code")
    print("status: ", status_code)
    # Databricks usually returns 200 or 204 for successful cancel
    if status_code in (200, 204):
        return {
            "status": "success",
            "message": f"Cancel request submitted for query {query_id}",
        }

    # Anything else is treated as failure
    error_msg = api_response.get("error") or str(api_response.get("body"))
    raise RuntimeError(f"Failed to cancel SQL query {query_id}. API Error: {error_msg}")
