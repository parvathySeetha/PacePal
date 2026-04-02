import asyncio
import os
import sys
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp_module.Salesforcemcp.client.sf_client import SalesforceClient
import json
import logging

# Set paths relative to this file (agents/ioagent/createRecords.py)
IOAGENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = IOAGENT_DIR.parents[1]


# Configure logger
logger = logging.getLogger(__name__)
f_client = SalesforceClient("marketing")
class GetRecordsTool:
    def __init__(self):
        pass

    async def create_records(self, object_name, fields):
        """
        Executes a SOQL query using the Salesforce MCP server and returns the records.
        """
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
                    
                    soql_data = await session.call_tool("upsert_salesforce_records", arguments={"object_name": object_name, "records": [{"record_id": None, "fields": fields}]}) 
                    logger.info(f"SoqlData: {soql_data}")
                    
                    if not soql_data.content:
                        return None
                        
                    # Assuming the tool returns a JSON string in the first text content
                    try:
                        return json.loads(soql_data.content[0].text)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON from tool output: {soql_data.content[0].text}")
                        return soql_data.content[0].text
                    except Exception as e:
                        logger.error(f"Error processing tool output: {e}")
                        return str(soql_data)

        except Exception as e:
            print(f"❌ Error connecting to server: {e}")
            logger.error(f"❌ Error connecting to server: {e}")
            return None

    async def create_records_bulk(self, object_name, records):
        """
        Executes a bulk creation request using the Salesforce MCP server.
        """
        server_path = PROJECT_ROOT / "mcp_module/Salesforcemcp/sf_server.py"
        server_params = StdioServerParameters(
            command=sys.executable,
            args=[str(server_path)],
        )

        print("🔌 Connecting to server for bulk creation...")
        logger.info("🔌 Connecting to server for bulk creation...")

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    
                    await session.initialize()
                    print("✅ Server initialized successfully!")
                    logger.info("✅ Server initialized successfully!")
                    
                    result = await session.call_tool("upsert_salesforce_records", arguments={"object_name": object_name , "records": [{"record_id": None, "fields": r} for r in records]}) 
                    logger.info(f"Bulk Creation Result: {result}")
                    
                    if not result.content:
                        return None
                        
                    # Assuming the tool returns a JSON string in the first text content
                    try:
                        return json.loads(result.content[0].text)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON from tool output: {result.content[0].text}")
                        return result.content[0].text
                    except Exception as e:
                        logger.error(f"Error processing tool output: {e}")
                        return str(result)

        except Exception as e:
            print(f"❌ Error connecting to server: {e}")
            logger.error(f"❌ Error connecting to server: {e}")
            return None

    async def upsert_record(self, object_name, record_id, fields):
        """
        Upserts a Salesforce record using the Salesforce MCP server.
        """
        server_path = PROJECT_ROOT / "mcp_module/Salesforcemcp/sf_server.py"
        server_params = StdioServerParameters(
            command=sys.executable,
            args=[str(server_path)],
        )

        print("🔌 Connecting to server for upsert...")
        logger.info("🔌 Connecting to server for upsert...")

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    
                    await session.initialize()
                    print("✅ Server initialized successfully!")
                    logger.info("✅ Server initialized successfully!")
                    
                    result = await session.call_tool("upsert_salesforce_records", arguments={"object_name": object_name, "records": [{"record_id": record_id, "fields": fields}]}) #changed
                    logger.info(f"Upsert Result: {result}")
                    
                    if not result.content:
                        return None
                        
                    # Assuming the tool returns a JSON string in the first text content
                    try:
                        return json.loads(result.content[0].text)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse JSON from tool output: {result.content[0].text}")
                        return result.content[0].text
                    except Exception as e:
                        logger.error(f"Error processing tool output: {e}")
                        return str(result)

        except Exception as e:
            print(f"❌ Error connecting to server: {e}")
            logger.error(f"❌ Error connecting to server: {e}")
            return None

def create_records(object_name, fields):
    tool = GetRecordsTool()
    return asyncio.run(tool.create_records(object_name, fields))

def create_records_bulk(object_name, records):
    tool = GetRecordsTool()
    return asyncio.run(tool.create_records_bulk(object_name, records))

def upsert_record(object_name, record_id, fields):
    tool = GetRecordsTool()
    return asyncio.run(tool.upsert_record(object_name, record_id, fields))
