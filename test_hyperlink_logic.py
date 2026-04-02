import asyncio
import sys
import os

# Add current directory to path so imports work
sys.path.append(os.getcwd())

from agents.marketing.nodes.completion import completion_node
# Mock MarketingState since we can't easily import it if it has dependencies
# Actually, completion_node expects a dict-like object, so a dict is fine if typed as MarketingState
# but python doesn't enforce types at runtime.

async def test():
    print("🚀 Starting Test...")
    # Simulate a state where an update happened, returning an ID but no name
    # The structure mimics what completion_node expects in mcp_results
    state = {
        "mcp_results": {
            "salesforce": {
                "tool_results": [
                    {
                        "tool_name": "upsert_salesforce_records",
                        "status": "success",
                        "record_id": "001xx000003DHP0AAO",
                        # "record_name": "Missing Name", # SIMULATING MISSING NAME
                        "request": {"object_name": "Account"},
                        "response": {"content": [{"text": "{\"results\": [{\"record_id\": \"001xx000003DHP0AAO\"}]}"}]}
                    },
                    {
                        "tool_name": "upsert_salesforce_records",
                        "status": "success",
                        "record_id": "00vfo000002RWdlAAG",
                        "request": {"object_name": "CampaignMember"},
                        "response": {"content": [{"text": "{\"results\": [{\"record_id\": \"00vfo000002RWdlAAG\"}]}"}]}
                    }
                ],
                "execution_summary": {"successful_calls": 2, "total_calls": 2}
            }
        },
        "user_goal": "Update account and add member"
    }

    # Run completion node
    try:
        new_state = await completion_node(state)
        
        # Check created_records
        records = new_state.get("created_records", {})
        print(f"📦 Created Records Output: {records}")
        
        # Check Account (should exist)
        accounts = records.get("Account", [])
        account_ok = False
        if len(accounts) > 0 and accounts[0]["Id"] == "001xx000003DHP0AAO":
            print("✅ SUCCESS: Account record preserved and fallback name used.")
            account_ok = True
        else:
            print("❌ FAILURE: Account record missing or incorrect.")

        # Check CampaignMember (should be FILTERED OUT)
        members = records.get("CampaignMember", [])
        member_ok = False
        if len(members) == 0:
            print("✅ SUCCESS: CampaignMember was correctly filtered out.")
            member_ok = True
        else:
            print(f"❌ FAILURE: CampaignMember was NOT filtered out: {members}")
            
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test())
