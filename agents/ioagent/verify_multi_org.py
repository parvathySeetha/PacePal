
import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

try:
    from mcp_module.Salesforcemcp.client.sf_client import SalesforceClient
    
    print("Initializing SalesforceClient with 'marketing'...")
    client_marketing = SalesforceClient("marketing")
    print(f"Marketing client initialized. Org type: {client_marketing.org_type}")
    
    print("Initializing SalesforceClient with 'agent'...")
    client_agent = SalesforceClient("agent")
    print(f"Agent client initialized. Org type: {client_agent.org_type}")
    
    if client_marketing is client_agent:
        print("ERROR: Clients should be different instances (unless singleton logic is per-org-type)")
    else:
        print("Clients are different instances (correct).")

    # Verify singleton behavior if implemented per org
    client_marketing_2 = SalesforceClient("marketing")
    if client_marketing is client_marketing_2:
        print("Singleton working for 'marketing' org.")
    else:
        print("Singleton NOT working for 'marketing' org (might be intended).")

    print("Verification successful.")

except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
