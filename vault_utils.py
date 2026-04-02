import hvac
import os
from dotenv import load_dotenv
load_dotenv()

# Initialize Vault client with URL and token from environment
vault_url = os.getenv('VAULT_URL', 'http://127.0.0.1:8200')
vault_token = os.getenv('VAULT_TOKEN')

if not vault_token:
    print(" WARNING: VAULT_TOKEN not found in environment variables!")
    print("   Please add VAULT_TOKEN to your .env file")

client = hvac.Client(url=vault_url, token=vault_token, timeout=5)
  
def read_secret(path: str, mount="secret"):
    try:
        resp = client.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount
        )
        return resp["data"]["data"]  # IMPORTANT: nested "data"
    except Exception as e:
        # Fallback to KV v1 if v2 fails (e.g. InvalidPath)
        try:
            resp = client.secrets.kv.v1.read_secret(
                path=path,
                mount_point=mount
            )
            return resp.get("data", {})
        except Exception as e2:
            print(":x: Error:", e2)
            return {}
