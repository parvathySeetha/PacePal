import asyncio
import os
import sys
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import json
import logging
from mcp_module.Salesforcemcp.client.sf_client import SalesforceClient

# Configure logger
logger = logging.getLogger(__name__)

# Set paths relative to this file
IOAGENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = IOAGENT_DIR.parent.parent

sf_client = SalesforceClient("marketing")
class GetRecordsTool:
    def __init__(self):
        pass

    async def get_records(self, soql_query):
        """
        Executes a SOQL query using the Salesforce MCP server and returns the records.
        """
        PROJECT_ROOT = IOAGENT_DIR.parent.parent
        server_path = PROJECT_ROOT / "mcp_module/Salesforcemcp/sf_server.py"
        server_params = StdioServerParameters(
            command=sys.executable,
            args=[str(server_path)],
        )

        print("🔌 Connecting to server...")
        logger.info("🔌 Connecting to server...")

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    
                    await session.initialize()
                    print("✅ Server initialized successfully!")
                    logger.info("✅ Server initialized successfully!")
                    
                    soql_data = await session.call_tool("run_dynamic_soql", arguments={"query": soql_query}) 
                    logger.info(f"SoqlData: {soql_data}")
                    
                    # Parse the JSON string from the text content
                    # The content is expected to be a list of TextContent objects, we take the first one.
                    if not soql_data.content:
                        return {}
                        
                    records_json = json.loads(soql_data.content[0].text)
                    
                    output_dict = {}
                    if 'records' in records_json:
                        for record in records_json['records']:
                            temp_dict = {}
                            for key, value in record.items():
                                temp_dict[key] = value
                            
                            if 'Id' in temp_dict:
                                output_dict[temp_dict['Id']] = temp_dict
                            else:
                                # Fallback if Id is missing, though unlikely for Salesforce records
                                # We might skip or use a hash, but standard practice implies Id is present if queried.
                                # If Id is not in query, we can't key by it. 
                                # Assuming Id is always selected or available.
                                pass
                                
                    logger.info(f"Fetched {len(output_dict)} records.")
                    return output_dict

        except Exception as e:
            print(f"\n❌ Error: {e}")
            logger.error(f"❌ Error in get_records: {e}")
            return {}

def get_records(soql_query):
    tool = GetRecordsTool()
    return asyncio.run(tool.get_records(soql_query))

if __name__ == "__main__":
    # Test
    query = "SELECT Id, Name FROM Account LIMIT 5"
    records = get_records(query)
    print(json.dumps(records, indent=2))
