from typing import Dict, Any
from client.sf_client import SalesforceClient
import logging

# Lazy initialization
_sf_client = None

def get_client():
    global _sf_client
    if not _sf_client:
        _sf_client = SalesforceClient("marketing")
        _sf_client.connect()
    return _sf_client

def get_session_info() -> Dict[str, Any]:
    """
    Retrieves the Salesforce session ID and instance URL.
    """
    try:
        client = get_client()
        if not client.sf:
            return {"error": "Salesforce connection not established"}
        
        return {
            "session_id": client.sf.session_id,
            "instance_url": client.sf.sf_instance,
            "success": True
        }
    except Exception as e:
        return {"error": f"Failed to retrieve session info: {str(e)}"}
