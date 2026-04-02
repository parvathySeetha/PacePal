import sys
from pathlib import Path

# Add parent directory to path
parent_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(parent_dir))

from vault_utils import read_secret

# Load Brevo secrets from Vault
linkly_secrets = read_secret("linkly")
 

CONFIG = {
    # Pull from Vault instead of .env
    "LINKLY_BASE_URL": linkly_secrets.get("LINKLY_BASE_URL", ""),
    "LINKLY_API_KEY": linkly_secrets.get("LINKLY_API_KEY", ""),
    "LINKLY_WORKSPACE":linkly_secrets.get("LINKLY_WORKSPACE")
}