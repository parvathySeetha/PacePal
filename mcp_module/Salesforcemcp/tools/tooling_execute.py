"""
Tooling Execute Tool Implementation for Salesforce MCP
Following the same pattern as run_dynamic_soql
"""
import json
import logging
from typing import Dict, Any, Optional

from Error.sf_error import SalesforceApiError
from client.sf_client import SalesforceClient

_sf_client = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("TEST_TOOLING_EXECUTE.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ],
    force=True
)

def get_client() -> SalesforceClient:
    """Get or create Salesforce client singleton."""
    global _sf_client
    if not _sf_client:
        _sf_client = SalesforceClient("marketing")
        _sf_client.connect()
    return _sf_client

def tooling_execute(action: str, method: str = "GET", data: Optional[Dict[str, Any]] = None):
    if not action:
        raise ValueError("Missing 'action' argument")

    sf_client = get_client()
    if not getattr(sf_client, "sf", None):
        raise ValueError("Salesforce connection not established.")

    try:
        results = sf_client.sf.toolingexecute(action, method=method, data=data)
    except SalesforceApiError as e:
        logging.exception("Tooling execute failed")
        return {"status": "error", "error": str(e)}

    # ✅ Return dict directly (less noisy, easier to parse)
    return {"result": results}
