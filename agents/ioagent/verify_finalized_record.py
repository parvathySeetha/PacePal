
import sys
import os
import json
from unittest.mock import MagicMock

from pathlib import Path

# Add the project directory to sys.path
sys.path.append(str(Path(__file__).parent.resolve()))

from datamodel import IOState, MediaCompany, ClientAgency, CampaignInformation, Terms, LineItem
from nodes import finalize

def test_finalize_node():
    # Mock State
    state = IOState(io_markdown="Mock IO")
    state.io_id = "IO-123"
    state.media_company = MediaCompany(name="Test Media")
    state.client_agency = ClientAgency(name="Test Client")
    state.campaign_information = CampaignInformation(campaign_name="Test Campaign", campaign_start_date="2025-01-01")
    state.terms = Terms(payment_term="Net 30")
    state.line_items = [LineItem(name="Line 1")]
    
    # Mock Matched Opportunity (Record, Score)
    state.matched_opportunity_records = [
        [{"Id": "Opp-001", "Name": "Best Opportunity", "Amount": 10000, "OwnerId": "Owner-001"}, 95],
        [{"Id": "Opp-002", "Name": "Second Best"}, 80]
    ]
    state.matched_opportunity_type = "perfect"
    
    # Mock Matched Account
    state.matched_account_records = [
        [{"Id": "Acc-001", "Name": "Best Account"}, 90]
    ]
    state.matched_account_type = "perfect"
    
    # Mock Matched Quote Line Items
    # Structure: List of dicts with "match" key containing the record
    # Simulating the nested structure found in logs
    state.matched_quote_line_items = [
        {
            "line_item_index": 0,
            "match": {
                "Id": "QLI-001",
                "QuoteLineItem": {
                    "QuoteId": "Q-999",
                    "Quote": {"Name": "Test Quote 2025"},
                    "PricebookEntry": {"Pricebook2Id": "PB-123"}
                }
            },
            "score": 95
        }
    ]
    state.matched_quote_type = "perfect"
    
    # Run finalize
    print("Running finalize node...")
    result = finalize(state)
    
    # Verify Output
    final_result = result.get("final_result")
    finalized_record = result.get("finalized_record")
    
    print("\n--- Verification Results ---")
    
    if finalized_record:
        print("Finalized Record found in output.")
        print(f"Opportunity Name: {finalized_record.opportunity.get('Name')}")
        print(f"Account Name: {finalized_record.account.get('Name')}")
        print(f"Quote ID: {finalized_record.quote_id}")
        print(f"Quote Name: {finalized_record.quote_name}")
        print(f"Pricebook ID: {finalized_record.pricebook_id}")
        
        # Assertions
        assert finalized_record.opportunity.get("Id") == "Opp-001"
        assert finalized_record.account.get("Id") == "Acc-001"
        assert finalized_record.quote_id == "Q-999"
        assert finalized_record.quote_name == "Test Quote 2025"
        assert finalized_record.pricebook_id == "PB-123"
        print("\nSUCCESS: All fields match expected values.")
    else:
        print("\nFAILURE: Finalized Record not found in output.")

    # Test create_salesforce_payload
    from nodes import create_salesforce_payload
    
    # Manually populate finalized_record in state for the test
    state.finalized_record = finalized_record
    
    print("\nRunning create_salesforce_payload node...")
    payload_result = create_salesforce_payload(state)
    payload = payload_result.get("salesforce_payload")
    
    if payload:
        print("Salesforce Payload generated.")
        print(json.dumps(payload, indent=2))
        
        assert payload["AccountId"] == "Acc-001"
        assert payload["EffectiveDate"] == "2025-01-01" # From Campaign Info
        assert payload["Name"] == "Test Client" # From Client Agency
        assert payload["OwnerId"] == "Owner-001" # From Opportunity
        assert payload["Pricebook2Id"] == "PB-123"
        assert payload["QuoteId"] == "Q-999"
        assert payload["Status"] == "Draft"
        
        print("\nSUCCESS: Payload matches expected values.")
    else:
        print("\nFAILURE: Salesforce Payload not generated.")

if __name__ == "__main__":
    test_finalize_node()
