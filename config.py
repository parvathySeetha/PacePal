import os
from dotenv import load_dotenv
load_dotenv()
SUPPORTED_ORGS = ["marketing", "agent", "demo", "io", "reconcile"]
def get_salesforce_config(org_type: str) -> dict:
    """
    Returns Salesforce credentials for the given org type
    using only environment variables from .env.
    """
    if org_type not in SUPPORTED_ORGS:
        raise ValueError(
            f"Unknown org_type: {org_type}. Use one of {SUPPORTED_ORGS}"
        )
    prefix_map = {
        "marketing": "MARKETING_",
        "agent": "AGENT_",
        "demo": "DEMO_",
        "io": "IO_",
        "reconcile": "RECONCILE_"
    }
    prefix = prefix_map[org_type]
    def get_required(key: str, default: str = "") -> str:
        return os.getenv(f"{prefix}{key}", default)
    return {
        "SALESFORCE_USERNAME": get_required("SALESFORCE_USERNAME"),
        "SALESFORCE_PASSWORD": get_required("SALESFORCE_PASSWORD"),
        "SALESFORCE_SECURITY_TOKEN": get_required("SALESFORCE_SECURITY_TOKEN"),
        "SALESFORCE_INSTANCE_URL": get_required("SALESFORCE_INSTANCE_URL"),
        "SALESFORCE_DOMAIN": get_required("SALESFORCE_DOMAIN", "login")
    }
 


