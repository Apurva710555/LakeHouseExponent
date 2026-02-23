# handlers.py
"""
Handlers module for Flask app — implements SCIM user/group operations using databricks_api.py
and records audit via logger_utils.append_audit.

Functions exported for app.py usage:
- construct_group_name
- create_user_handler
- update_user_handler
- delete_user_handler
- create_group_handler
- add_members_to_group_handler
- remove_members_from_group_handler
- update_group_handler
"""

from typing import Dict, Any, List, Optional, Set
import json
import math
import traceback
import os
from databricks_api import *
from logger_utils import append_audit
from databricks_api import (
    create_all_purpose_cluster,
    get_policy_id,
)     #added by Roshan
 

# Constants
MAX_GROUP_BATCH = 100  # similar to the notebook
# Accept either comma or semicolon separators for member lists
_MEMBER_SEPARATORS = [",", ";"]


# -------------------------
# Helpers
# -------------------------
def _split_members(members_csv: Optional[str]) -> List[str]:
    if not members_csv:
        return []
    csv = members_csv.strip()
    for sep in _MEMBER_SEPARATORS:
        if sep in csv:
            parts = [p.strip().lower() for p in csv.split(sep) if p.strip()]
            return parts
    # single value
    return [csv.lower()] if csv else []


def construct_group_name(row: Dict[str, Any]) -> str:
    """
    Construct group name.
    Notebook used: env prefix 'p' if env indicates production (e.g. 'prd' or 'Production' or 'prod'),
    else 'np'. Then join: <env>_TM<bu>_<domain>_<role>
    """
    env_raw = (row.get("env") or "").strip().lower()
    # treat several forms as production
    is_prd = env_raw in {"prod", "production", "prd", "p"}
    env = "prod" if is_prd else "nprod"

    bu = (row.get("bu") or "").strip()
    domain = (row.get("domain") or "").strip()
    appName = (row.get("appName") or "").strip()
    role = (row.get("role") or "").strip()

    # Use other_type value if bu is 'Others', otherwise use bu
    if bu == "Others":
        bu_value = (row.get("other_type") or "").strip()
        if not bu_value:
            raise ValueError("other_type is required when bu is 'Others'")
    else:
        bu_value = bu

    parts = [env]
    if bu_value:
        parts.append(f"TM{bu_value}")
    if domain:
        parts.append(domain)
    if appName:
        parts.append(appName)
    if role:
        parts.append(role)

    return "_".join(parts)


def _find_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Return SCIM user resource dict or None."""
    if not email:
        return None
    try:
        # databricks_api.scim_list_users returns {status_code, body}
        # Attempt 1: match on userName (typical)
        filt = f'userName eq "{email}"'
        resp = scim_list_users(filt)
        if resp and resp.get("status_code") == 200:
            body = resp.get("body") or {}
            resources = body.get("Resources") or []
            if resources:
                return resources[0]

        # Attempt 2: match on emails.value (some directories populate emails but not userName)
        filt_alt = f'emails.value eq "{email}"'
        resp_alt = scim_list_users(filt_alt)
        if resp_alt and resp_alt.get("status_code") == 200:
            body_alt = resp_alt.get("body") or {}
            resources_alt = body_alt.get("Resources") or []
            return resources_alt[0] if resources_alt else None
        return None
    except Exception:
        # bubble up or return None — handlers will audit
        return None


def get_user_id_by_email(email: str) -> Optional[str]:
    u = _find_user_by_email(email.strip().lower())
    return u.get("id") if u else None


def ensure_user(email: str, display_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Ensure a user exists: if present -> return {'existed': True, 'id': id}
    otherwise try to create and return {'existed': False, 'id': id, 'resp': resp}
    """
    email_norm = (email or "").strip().lower()
    if not email_norm:
        return {"existed": False, "id": None, "error": "no_email_provided"}

    existing = _find_user_by_email(email_norm)
    if existing:
        return {"existed": True, "id": existing.get("id")}

    # build a minimal create payload (SCIM)
    display = (display_name or email_norm).strip()
    parts = display.split(" ", 1)
    if len(parts) == 1:
        given, family = parts[0], ""
    else:
        given, family = parts[0], parts[1]

    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email_norm,
        "name": {"givenName": given, "familyName": family, "formatted": display},
        "emails": [{"value": email_norm, "primary": True}],
        "active": True,
    }
    try:
        resp = scim_create_user(payload)
        if resp.get("status_code") not in (200, 201):
            raise RuntimeError(str(resp.get("body")))
        body = resp.get("body") or {}
        user_id = body.get("id")
        append_audit("CREATE_USER", "user", email_norm, "SUCCESS", "created", response_code=resp.get("status_code"), response_body=resp, request_payload=payload)
        return {"existed": False, "id": user_id, "resp": resp}
    except Exception as e:
        append_audit("CREATE_USER", "user", email_norm, "FAILED", str(e), request_payload=payload)
        return {"existed": False, "id": None, "error": str(e)}


def _find_group_by_display_name(display_name: str) -> Optional[Dict[str, Any]]:
    if not display_name:
        return None
    try:
        resp = scim_list_groups(f'displayName eq "{display_name}"')
        if not resp or resp.get("status_code") != 200:
            return None
        body = resp.get("body") or {}
        resources = body.get("Resources") or []
        return resources[0] if resources else None
    except Exception:
        return None


def _chunked(iterable: List[Any], size: int):
    """Yield successive chunks of given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


# -------------------------
# Handlers (exported)
# -------------------------
def create_user_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """Create a user (idempotent)."""
    email = (row.get("user_email") or "").strip().lower()
    first_name = (row.get("first_name") or "").strip()
    last_name = (row.get("last_name") or "").strip()
    if first_name or last_name:
        display_name = (first_name + (" " + last_name if last_name else "")).strip()
    else:
        display_name = email

    if not email:
        append_audit("CREATE_USER", "user", "", "FAILED", "user_email missing")
        raise ValueError("user_email required")

    existing = _find_user_by_email(email)
    if existing:
        append_audit("CREATE_USER", "user", email, "SKIPPED", "already_exists")
        return {"status": "skipped", "id": existing.get("id")}

    result = ensure_user(email, display_name)
    if result.get("id"):
        return {"status": "created", "id": result.get("id"), "resp": result.get("resp")}
    else:
        raise RuntimeError(f"create_user failed: {result.get('error')}")


def update_user_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """Update user attributes via SCIM Patch (patch ops should be provided or built)."""
    email = (row.get("user_email") or "").strip().lower()
    if not email:
        append_audit("UPDATE_USER", "user", "", "FAILED", "user_email required")
        raise ValueError("user_email required")

    existing = _find_user_by_email(email)
    if not existing:
        append_audit("UPDATE_USER", "user", email, "NOT_FOUND", "user not found")
        raise ValueError("user not found")

    user_id = existing.get("id")
    # Build patch operations from row attributes_json or user_name
    ops = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"], "Operations": []}

    # If attributes_json provided, try to parse it (expects JSON with SCIM-like keys)
    attrs_json = (row.get("attributes_json") or "").strip()
    if attrs_json:
        try:
            attrs = json.loads(attrs_json)
            # support a small subset: displayName, active
            if "displayName" in attrs:
                ops["Operations"].append({"op": "replace", "path": "displayName", "value": attrs["displayName"]})
            if "active" in attrs:
                ops["Operations"].append({"op": "replace", "path": "active", "value": bool(attrs["active"])})
        except Exception as e:
            append_audit("UPDATE_USER", "user", email, "FAILED", f"invalid attributes_json: {e}", request_payload=attrs_json)
            raise

    # allow first_name/last_name to update name
    first_name = (row.get("first_name") or "").strip()
    last_name = (row.get("last_name") or "").strip()
    composed = (first_name + (" " + last_name if last_name else "")).strip() if (first_name or last_name) else ""
    if composed and not any(op for op in ops["Operations"] if op.get("path") in ("displayName", "name.formatted")):
        ops["Operations"].append({"op": "replace", "path": "name.formatted", "value": composed})

    if not ops["Operations"]:
        append_audit("UPDATE_USER", "user", email, "NOOP", "nothing to update")
        return {"status": "noop"}

    try:
        resp = scim_patch_user(user_id, ops)
        append_audit("UPDATE_USER", "user", email, "SUCCESS", "patched", response_code=200, response_body=resp, request_payload=ops)
        return {"status": "updated", "resp": resp}
    except Exception as e:
        append_audit("UPDATE_USER", "user", email, "FAILED", str(e), request_payload=ops)
        raise


def delete_user_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """Delete a user by id or email."""
    user_id = (row.get("user_id") or "").strip()
    if not user_id:
        email = (row.get("user_email") or row.get("email") or "").strip().lower()
        if not email:
            append_audit("DELETE_USER", "user", "", "FAILED", "user_id or user_email required")
            raise ValueError("user_id or user_email required")
        existing = _find_user_by_email(email)
        if not existing:
            append_audit("DELETE_USER", "user", email, "NOT_FOUND", "")
            raise ValueError("user not found")
        user_id = existing.get("id")

    try:
        scim_delete_user(user_id)
        append_audit("DELETE_USER", "user", user_id, "SUCCESS", "deleted", response_code=204)
        return {"status": "deleted", "id": user_id}
    except Exception as e:
        append_audit("DELETE_USER", "user", user_id, "FAILED", str(e))
        raise


def create_group_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a group. Members in row['group_members'] may be emails (we attempt to resolve to user ids)
    but SCIM group creation accepts either user ids or member objects. We'll try to create with
    members resolved to IDs when possible.
    """
    group_name = construct_group_name(row)
    if not group_name:
        append_audit("CREATE_GROUP", "group", "", "FAILED", "cannot construct group_name from row data")
        raise ValueError("group_name required")

    # If a group with the same displayName already exists, raise error
    existing_group = _find_group_by_display_name(group_name)
    if existing_group:
        msg = "Group already exists. Use add_to_group to add members."
        append_audit("CREATE_GROUP", "group", group_name, "FAILED", msg)
        raise ValueError(msg)

    members = _split_members(row.get("group_members") or "")
    member_objs = []
    missing_members = []
    for mem in members:
        uid = get_user_id_by_email(mem)
        if uid:
            member_objs.append({"value": uid})
        else:
            missing_members.append(mem)

    if missing_members:
        msg = f"Cannot create group. These users do not exist: {', '.join(missing_members)}"
        append_audit("CREATE_GROUP", "group", group_name, "FAILED", msg)
        raise ValueError(msg)

    # Create group with displayName only (SCIM often rejects members in create)
    create_payload = {"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"], "displayName": group_name}
    print(create_payload)
    try:
        resp = scim_create_group(create_payload)
        status = resp.get("status_code")
        if status not in (200, 201):
            raise RuntimeError(str(resp.get("body")))
        append_audit("CREATE_GROUP", "group", group_name, "SUCCESS", "created", response_code=status, response_body=resp, request_payload=create_payload)
    except Exception as e:
        append_audit("CREATE_GROUP", "group", group_name, "FAILED", str(e), request_payload=create_payload)
        raise

    # Add members via PATCH after successful creation
    if member_objs:
        body = resp.get("body") or {}
        group_id = body.get("id")
        patch_payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "add", "path": "members", "value": member_objs}],
        }
        try:
            patch_resp = scim_patch_group(group_id, patch_payload)
            patch_status = patch_resp.get("status_code")
            # PATCH may return 200 or 204 for success
            if patch_status not in (200, 204):
                raise RuntimeError(str(patch_resp.get("body")))
            append_audit("CREATE_GROUP", "group", group_name, "SUCCESS", f"added {len(member_objs)} members", response_code=patch_status, response_body=patch_resp, request_payload=patch_payload)
            return {"status": "created", "resp": resp, "members_patched": True}
        except Exception as e:
            append_audit("CREATE_GROUP", "group", group_name, "FAILED", f"members patch failed: {e}", request_payload=patch_payload)
            # Surface error; caller/front-end can decide next action
            raise

    return {"status": "created", "resp": resp, "members_patched": False}


def add_members_to_group_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add members to an existing group. Members provided as emails or ids.
    Uses SCIM PATCH add "members".
    """
    group_name = construct_group_name(row)
    if not group_name:
        append_audit("ADD_TO_GROUP", "group", "", "FAILED", "cannot construct group_name")
        raise ValueError("group_name required")

    members = _split_members(row.get("group_members") or "")
    if not members:
        append_audit("ADD_TO_GROUP", "group", group_name, "NOOP", "no members specified")
        return {"status": "noop"}

    group = _find_group_by_display_name(group_name)
    if not group:
        append_audit("ADD_TO_GROUP", "group", group_name, "NOT_FOUND", "group not found")
        raise ValueError("group not found")

    group_id = group.get("id")
    # Resolve to IDs; if any missing, error out (do not auto-create)
    member_objs = []
    missing_members = []
    for m in members:
        uid = get_user_id_by_email(m)
        if uid:
            member_objs.append({"value": uid})
        else:
            missing_members.append(m)

    if missing_members:
        msg = f"Cannot add members. These users do not exist: {', '.join(missing_members)}"
        append_audit("ADD_TO_GROUP", "group", group_name, "FAILED", msg)
        raise ValueError(msg)

    patch = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"], "Operations": [{"op": "add", "path": "members", "value": member_objs}]}

    try:
        resp = scim_patch_group(group_id, patch)
        status = resp.get("status_code")
        if status not in (200, 204):
            raise RuntimeError(str(resp.get("body")))
        append_audit("ADD_TO_GROUP", "group", group_name, "SUCCESS", f"added {len(member_objs)}", response_code=status, response_body=resp, request_payload=patch)
        return {"status": "added", "resp": resp}
    except Exception as e:
        append_audit("ADD_TO_GROUP", "group", group_name, "FAILED", str(e), request_payload=patch)
        raise


def remove_members_from_group_handler(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove members from group. Accepts member list as emails or ids.
    For each member we build a remove operation path: members[value eq "<id>"]
    Batch removal is done one-by-one to accommodate SCIM filtering remove expression.
    """
    group_name = construct_group_name(row)
    if not group_name:
        append_audit("REMOVE_FROM_GROUP", "group", "", "FAILED", "cannot construct group_name")
        raise ValueError("group_name required")

    members = _split_members(row.get("group_members") or "")
    if not members:
        append_audit("REMOVE_FROM_GROUP", "group", group_name, "NOOP", "no members specified")
        return {"status": "noop"}

    group = _find_group_by_display_name(group_name)
    if not group:
        append_audit("REMOVE_FROM_GROUP", "group", group_name, "NOT_FOUND", "group not found")
        raise ValueError("group not found")

    group_id = group.get("id")
    results = []
    for m in members:
        uid = get_user_id_by_email(m)
        if not uid:
            # if given an id-like string, maybe it's already an id; try to treat as id
            candidate = m
            uid = candidate if candidate else None
        if not uid:
            # nothing we can do for this member
            append_audit("REMOVE_FROM_GROUP", "group", group_name, "SKIPPED", f"user not found {m}", request_payload=m)
            results.append({"member": m, "status": "skipped"})
            continue
        ops = [{"op": "remove", "path": f'members[value eq "{uid}"]'}]
        patch = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"], "Operations": ops}
        try:
            resp = scim_patch_group(group_id, patch)
            append_audit("REMOVE_FROM_GROUP", "group", group_name, "SUCCESS", f"removed {uid}", response_code=200, response_body=resp, request_payload=patch)
            results.append({"member": m, "id": uid, "status": "removed"})
        except Exception as e:
            append_audit("REMOVE_FROM_GROUP", "group", group_name, "FAILED", f"remove {uid} failed: {e}", request_payload=patch)
            results.append({"member": m, "id": uid, "status": "failed", "error": str(e)})

    return {"status": "partial" if any(r.get("status") != "removed" for r in results) else "removed_all", "results": results}


def _get_env_config(environment: str) -> tuple[str, str, str]:
    """
    Maps the environment name to modifiers used in resource naming.
    """
    
    # 1. Normalize the input environment string 
    env_key = environment.strip().lower()
      
    env_config = {
        'dev': {'aws_s3_modifier': 'nonprod', 'catalog_modifier': 'd'},
        'prod': {'aws_s3_modifier': 'prod', 'catalog_modifier': 'p'}
    }
    
    if env_key in env_config:
        aws_s3_modifier = env_config[env_key]['aws_s3_modifier']
        catalog_modifier = env_config[env_key]['catalog_modifier']
        return env_key, aws_s3_modifier, catalog_modifier
    else:
        raise ValueError(
            f"Invalid environment specified: {environment}. Must resolve to 'dev' or 'prod'."
        )

# def onboard_project_handler(row: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Executes the Databricks Project Onboarding workflow, provisioning all
#     required resources (folders, clusters, SQL warehouse, groups, and permissions).
    
#     This version reuses add_members_to_group_handler to add the Application Owner.
#     """
#     # 1. Configuration & Host Setup and Validation
#     app_name = row.get("application_name", "").strip()
#     environment_raw = row.get("environment", "").strip()
#     organization_name = row.get("organization_name", "").strip()
#     department = row.get("department", "").strip()
#     application_owner_email = row.get("application_owner_email", "").strip()
    
#     if not app_name:
#         raise ValueError("Application Name is required for project onboarding.")
#     if not environment_raw:
#         raise ValueError("Environment (dev/prod) is required for project onboarding.")
#     if not organization_name:
#         raise ValueError("Organization Name is required for project onboarding.")

#     # 2. Input Parsing and Validation (Remaining validation checks)
#     MAX_ORG_LENGTH = 10
#     if len(organization_name) > MAX_ORG_LENGTH:
#         raise ValueError(f"Organization name length ({len(organization_name)}) exceeds max of {MAX_ORG_LENGTH}.")
#     if organization_name.lower() in department.lower():
#         raise ValueError(f"Department name ('{department}') must NOT contain Organization name ('{organization_name}').")
        
#     # 3. Map Configs and Tags
#     environment, aws_s3_modifier, catalog_modifier = _get_env_config(environment_raw)

#     # --- UC NAMING LOGIC ---
#     application_schema_name = app_name.lower()
#     department_lower = row.get("department", "").strip().lower()
    
#     if not department_lower:
#         raise ValueError("Department is required for Unity Catalog naming.")

#     # 1. Define S3 Root: s3://cv-deep-<department>-<nonprod/prod>
#     #https://deep-platform-management.s3.ap-south-1.amazonaws.com/createuser.csv
#     S3_BUCKET_BASE = f"s3://deep-platform-mnagement"
#     #S3_BUCKET_BASE = f"s3://cv-deep-{department_lower}-{aws_s3_modifier}"
    
#     # 2. Define Schema Path: /<Application Name>
#     SCHEMA_SUB_PATH = app_name
                                     
#     # FINAL STORAGE ROOT URL:
#     STORAGE_ROOT_URL = f"{S3_BUCKET_BASE}/{SCHEMA_SUB_PATH}"
    
#     # Define UC Names 
#     UC_CATALOG_NAME = f"{catalog_modifier}_{department_lower}" 
#     UC_SCHEMA_FULL_NAME = f"{UC_CATALOG_NAME}.{application_schema_name}"
#     # --- END UC NAMING LOGIC ---
    
#     project_tags = {
#         "application_name": app_name,
#         "application_owner": row.get("application_owner", "").strip(),
#         "business_owner": row.get("business_owner", "").strip(),
#         "cost_center": row.get("cost_center", "").strip(),
#         "department": department
#     }
    
#     cluster_type = row.get("cluster_type")
#     node_type_id = row.get("node_type_id")
#     base_workers = row.get("base_workers")
#     sql_wh_size = row.get("sql_wh_size")
#     POLICY_NAME_TO_USE = "Unified-All-Purpose-Compute" if cluster_type == "all-purpose" else "Unified-Job-Compute"

#     # Permission levels
#     ADMIN_PERMISSION = "CAN_MANAGE"
#     CONTRIBUTOR_FILE_PERMISSION = "CAN_EDIT"
#     READER_FILE_PERMISSION = "CAN_READ"
#     CONTRIBUTOR_CLUSTER_PERMISSION = "CAN_RESTART" 
#     READER_CLUSTER_PERMISSION = "CAN_ATTACH_TO"
#     CONTRIBUTOR_WH_PERMISSION = "CAN_USE" 
#     READER_WH_PERMISSION = "CAN_VIEW"
    
#     resource_ids = {
#         "folder_path": None, 
#         "policy_id": None, 
#         "cluster_id": None, 
#         "wh_id": None, 
#         "groups": {},
#         "uc_catalog": UC_CATALOG_NAME,
#         "uc_schema_full_name": UC_SCHEMA_FULL_NAME,
#         "uc_storage_root": STORAGE_ROOT_URL
#     }

#     # 4. Core Workflow Execution (Resource Creation)
    
#     # 4.1. Create Project Folder
#     api_response, folder_path = create_project_folder(app_name)
#     if api_response.get("status_code") in (200, 201):
#         resource_ids["folder_path"] = folder_path
#     else: 
#         error_msg = api_response.get("error") or str(api_response.get("body"))
#         raise RuntimeError(f"Failed to create project folder: {error_msg}")

#     # # 4.2. Create Unity Catalog Schema
#     # try:
#     #     schema_result = create_project_schema(
#     #         UC_CATALOG_NAME, 
#     #         application_schema_name, 
#     #         STORAGE_ROOT_URL
#     #     )
#     #     resource_ids["uc_schema_status"] = schema_result.get("status", "created") 
#     # except Exception as e:
#     #     append_audit("ONBOARD_PROJECT", "unity_catalog", app_name, "FAILED", f"Schema creation failed: {e}")
#     #     raise RuntimeError(f"Failed to create Unity Catalog schema: {e}")
    
#     # group_names_to_create = build_group_name(organization_name, department, app_name, environment)

#     # 4.3. Get Policy ID
#     policy_id = get_policy_id(POLICY_NAME_TO_USE)
#     if policy_id:
#         resource_ids["policy_id"] = policy_id

#     # 4.4. Create Cluster
#     cluster_id = None
#     if policy_id:
#        cluster_id = create_all_purpose_cluster(app_name, policy_id, node_type_id, base_workers, environment, project_tags)
#        if cluster_id:
#            resource_ids["cluster_id"] = cluster_id
    
#     # 4.5. Create SQL Warehouse
#     wh_id = create_sql_warehouse(app_name, sql_wh_size, project_tags)
#     if wh_id:
#         resource_ids["wh_id"] = wh_id

#     # 5. Group Creation and Permissions Assignment
#     folder_acl, cluster_acl, sql_wh_acl = [], [], []

#     # for group_name in group_names_to_create:
#     #     group_id = create_project_groups(group_name)
        
#     #     if group_id:
#     #         resource_ids["groups"][group_name] = group_id
            
#     #         # Determine Permissions (Remains the same)
#     #         if group_name.endswith("_administrators"):
#     #            f_p, c_p, w_p = ADMIN_PERMISSION, ADMIN_PERMISSION, ADMIN_PERMISSION
#     #         elif group_name.endswith("_contributors"):
#     #            f_p, c_p, w_p = CONTRIBUTOR_FILE_PERMISSION, CONTRIBUTOR_CLUSTER_PERMISSION, CONTRIBUTOR_WH_PERMISSION
#     #         elif group_name.endswith("_readers"):
#     #            f_p, c_p, w_p = READER_FILE_PERMISSION, READER_CLUSTER_PERMISSION, READER_WH_PERMISSION
#     #         else:  
#     #             continue

#     #         # Collect Permissions (Remains the same)
#     #         folder_acl.append({"group_name": group_name, "permission_level": f_p})
#     #         cluster_acl.append({"group_name": group_name, "permission_level": c_p})
#     #         sql_wh_acl.append({"group_name": group_name, "permission_level": w_p})

#     #         # Add Application Owner
#     #         add_user_to_group_by_id(group_id, group_name, application_owner_email)

#     # Consolidated Permission Assignment
#     if folder_path: 
#         set_all_folder_permissions(folder_path, folder_acl)
#     if cluster_id: 
#         set_all_cluster_permissions(cluster_id, cluster_acl) 
#     if wh_id: 
#        set_all_sql_warehouse_permissions(wh_id, sql_wh_acl)
    
#     # 7. Final Auditing and Return
#     append_audit(
#         "ONBOARD_PROJECT", 
#         "project", 
#         app_name, 
#         "SUCCESS", 
#         "All resources provisioned and permissions set.", 
#         response_body=resource_ids
#     )

#     return {
#         "application_name": app_name,
#         "application_owner_email": application_owner_email,
#         "environment": environment,
#         "application_owner": row.get("application_owner", "").strip(),
#         "folder_path": folder_path,
#         "policy_id": policy_id,
#         "cluster_id": cluster_id,
#         "sql_wh_id": wh_id,
#         "groups": list(resource_ids["groups"].keys()),
#         "uc_schema_full_name": UC_SCHEMA_FULL_NAME
#     }

# # UPDATE_GROUP removed by request

def onboard_project_handler(row: Dict[str, Any]) -> Dict[str, Any]:
 
    app_name = (row.get("application_name") or "").strip()
    cluster_type = (row.get("cluster_type") or "").strip().lower()
    node_type_id = (row.get("node_type_id") or "").strip()
    base_workers = row.get("base_workers")
    sql_wh_size = row.get("sql_wh_size")
    environment = (row.get("environment") or "").strip().lower()
 
    if not app_name:
        raise ValueError("Application name is required")
 
    project_tags = {
        "application_name": app_name,
        "environment": environment,
        "cost_center": row.get("cost_center", ""),
        "department": row.get("department", ""),
        "business_owner": row.get("business_owner", ""),
    }
 
    try:
 
        # ===============================
        # ALL PURPOSE → CREATE CLUSTER
        # ===============================
        if cluster_type == "all-purpose":
 
            POLICY_NAME_TO_USE = "Unified-All-Purpose-Compute"
            policy_id = get_policy_id(POLICY_NAME_TO_USE)
 
            cluster_id = create_all_purpose_cluster(
                application_name=app_name,
                policy_id=policy_id,
                node_type_id=node_type_id,
                base_workers=base_workers,
                environment=environment,
                project_tags=project_tags,
            )
 
            append_audit(
                "ONBOARD_PROJECT",
                "cluster",
                app_name,
                "SUCCESS",
                f"All-purpose cluster created: {cluster_id}"
            )
 
            return {
                "application_name": app_name,
                "cluster_id": cluster_id,
                "cluster_type": "all-purpose",
                "environment": environment,
                "status": "SUCCESS",
            }
 
        # ===============================
        # JOB → CREATE SQL WAREHOUSE
        # ===============================
        elif cluster_type == "job":
 
            wh_id = create_sql_warehouse(
                application_name=app_name,
                sql_wh_size=sql_wh_size,
                project_tags=project_tags
            )
 
            append_audit(
                "ONBOARD_PROJECT",
                "sql_warehouse",
                app_name,
                "SUCCESS",
                f"SQL Warehouse created: {wh_id}"
            )
 
            return {
                "application_name": app_name,
                "sql_wh_id": wh_id,
                "cluster_type": "job",
                "environment": environment,
                "status": "SUCCESS",
            }
 
        else:
            raise ValueError(f"Invalid cluster_type received: {cluster_type}")
 
    except Exception as e:
 
        append_audit(
            "ONBOARD_PROJECT",
            "project",
            app_name,
            "FAILED",
            str(e)
        )
 
        raise ValueError(f"Invalid cluster_type received: {cluster_type}")

def get_dashboards_list_handler() -> List[Dict[str, Any]]:
 
    dashboards = get_dashboards_list()
    return {"status": "success", "dashboard_list": dashboards}
# end of file