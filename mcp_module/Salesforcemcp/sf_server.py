from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from Error.sf_error import SalesforceApiError
from tools import (
    run_dynamic_soql,
    delete_salesforce_record,
    generate_all_toolinput,
    propose_action,
    upsert_salesforce_records,
    tooling_execute,
    get_session_info
)


# Initialize MCP
mcp = FastMCP("salesforce-mcp")
mcp.tool()(run_dynamic_soql)
mcp.tool()(delete_salesforce_record)
mcp.tool()(generate_all_toolinput)
mcp.tool()(propose_action)
mcp.tool()(upsert_salesforce_records)
mcp.tool()(tooling_execute)
mcp.tool()(get_session_info)


def main():
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
