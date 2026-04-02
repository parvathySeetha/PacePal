import json
from unittest.mock import MagicMock, patch, AsyncMock
import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Add current directory to sys.path
sys.path.append(os.getcwd())

# Set dummy API key for ChatOpenAI init
os.environ["OPENAI_API_KEY"] = "dummy"

from datamodel import IOState, MediaCompany, ClientAgency, CampaignInformation, Terms, LineItem
from io_agent2 import io_agent

def mock_llm_invoke(messages):
    content = messages[0].content
    response = MagicMock()
    
    if "extract specific entities" in content:
        response.content = json.dumps({
            "media_company": {"name": "PT ADA Asia Indonesia"},
            "client_agency": {"name": "Test Client", "type": "Advertiser"},
            "campaign_information": {"campaign_name": "Test Campaign"},
            "terms": {"payment_term": "Net 30"},
            "io_id": "IO-12345"
        })
    elif "Extract all line items" in content:
        response.content = json.dumps([
            {"product_code": "P1", "name": "Product 1", "start_date": "2023-01-01", "end_date": "2023-01-31", "budget": 1000}
        ])
    elif "Dynamic Mapping JSON" in content:
        # Opportunity Mapping
        response.content = json.dumps([
            ["Test Campaign", ["Name"], 5]
        ])
    elif "Generate a SOQL query to fetch Account details" in content:
        response.content = "SELECT Id, Name FROM Account WHERE Id = 'ACC-123'"
    elif "match Client/Agency data against Salesforce Account fields" in content:
        response.content = json.dumps([
            ["Test Client", ["Name"], 5]
        ])
    elif "Generate a SOQL query to retrieve the synced Quote" in content:
        response.content = "SELECT Id, SegmentName, Product2.Name, ListPrice, Quantity, StartDate, EndDate FROM QuoteLineItem WHERE Quote.OpportunityId = 'OPP-123'"
    elif "match IO Line Items against Salesforce QuoteLineItem fields" in content:
        response.content = json.dumps([
            ["Product 1", ["Product2.Name"], 5]
        ])
    else:
        response.content = "{}"
        
    return response

def mock_run_similarity_analysis(soql, input_data):
    input_str = str(input_data)
    if "Product 1" in input_str:
        return {
            "QLI-123": [[{"Id": "QLI-123", "Product2": {"Name": "Product 1"}}], 95]
        }
    elif "Test Client" in input_str:
        return {
            "ACC-123": [[{"Id": "ACC-123", "Name": "Test Client"}], 95]
        }
    elif "Test Campaign" in input_str:
        return {
            "OPP-123": [[{"Id": "OPP-123", "Name": "Test Campaign"}], 95]
        }
    return {}

@patch("nodes.llm")
@patch("nodes.Similarityanalysis.similarutyanalysistool")
@patch("nodes.Similarityanalysis.run_similarity_analysis", side_effect=mock_run_similarity_analysis)
def test_agent(mock_similarity_func, mock_similarity_tool_cls, mock_llm):
    print("Starting Mock Test...")
    
    # Configure mock_llm.invoke to return the mock response
    mock_llm.invoke.side_effect = mock_llm_invoke
    
    # Configure mock tool instance
    mock_tool_instance = mock_similarity_tool_cls.return_value
    
    # Mock getSoqlData (AsyncMock)
    mock_tool_instance.getSoqlData = AsyncMock(return_value={"QLI-123": {"Id": "QLI-123", "Product2": {"Name": "Product 1"}}})
    
    # Mock run_with_data
    def mock_run_with_data(soql_data, input_data):
        # input_data is a list of mappings for ONE line item
        # e.g. [['Product 1', ['Product2.Name'], 5]]
        input_str = str(input_data)
        if "Product 1" in input_str:
            return {
                "QLI-123": [[{"Id": "QLI-123", "Product2": {"Name": "Product 1"}}], 95]
            }
        return {}
    mock_tool_instance.run_with_data.side_effect = mock_run_with_data

    initial_state = IOState(io_markdown="Mock IO Content")
    result = io_agent.invoke(initial_state)
    
    print("\nTest Result:")
    print(json.dumps(result["final_result"], indent=2))
    
    # Assertions
    assert result["io_id"] == "IO-12345"
    assert len(result["line_items"]) == 1
    
    # Debug Opportunity
    opp_type = result["final_result"]["matching"]["opportunity"]["type"]
    print(f"Opportunity Type: {opp_type}")
    if opp_type != "perfect":
        print(f"Opportunity Records: {result['final_result']['matching']['opportunity']['records']}")
    
    assert opp_type == "perfect"
    assert result["final_result"]["matching"]["account"]["type"] == "perfect"
    
    # Quote matching logic changed to list of results
    quote_results = result["final_result"]["matching"]["quote"]["records"]
    assert len(quote_results) == 1
    assert quote_results[0]["score"] == 95
    assert result["final_result"]["matching"]["quote"]["type"] == "perfect"
    
    print("\n✅ Test Passed!")

if __name__ == "__main__":
    test_agent()
