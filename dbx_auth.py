import os
import time
import requests
from dotenv import load_dotenv

# Load environment variables early
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"[INFO] Loaded .env from {dotenv_path}")
else:
    print("[WARN] .env file not found — relying on Databricks environment variables")

ACCOUNT_HOST = os.getenv("DATABRICKS_INSTANCE")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

_token_cache = {"token": None, "expires_at": 0}

def get_dbx_access_token():
    # Reuse token if still valid
    if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]

    url = f"{ACCOUNT_HOST}/oidc/accounts/{ACCOUNT_ID}/v1/token"

    data = {
        "grant_type": "client_credentials",
        "scope": "all-apis"
    }

    resp = requests.post(
        url,
        data=data,
        auth=(CLIENT_ID, CLIENT_SECRET),
        timeout=15
    )
    resp.raise_for_status()

    token_data = resp.json()

    _token_cache["token"] = token_data["access_token"]
    _token_cache["expires_at"] = time.time() + token_data["expires_in"] - 60

    return _token_cache["token"]
