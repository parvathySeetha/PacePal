from datamodel import FinalizedRecord
import os
import json
import logging
from dotenv import load_dotenv
from langgraph.graph import StateGraph, START, END

logger = logging.getLogger(__name__)

from datamodel import IOState
from nodes import (
    extract_header,
    validate_header,
    retry_header,
    extract_line_items,
    validate_line_items,
    retry_line_items,
    process_multiple_attachments,
    build_order_payload_agent,
    insert_order_mcp,
    build_line_items_payload_agent,
    build_line_items_payload_agent,
    insert_line_items_mcp,
    check_user_intent,
    get_attachments,
    process_multiple_attachments,
    download_and_convert_attachment,
    extract_header,
    validate_header,
    retry_header,
    extract_line_items,
    validate_line_items,
    retry_line_items,
    similarity_analysis_json_builder,
    call_similarity_analysis,
    get_account_soql,
    account_similarity_analysis_json_builder,
    call_account_similarity_analysis,
    generate_quote_soql,
    validate_line_items_loop,
    finalize,
    finalize,
    error_handler,
    handle_user_selection_of_campaign,
    return_status_of_order_items
)

# Load environment variables
load_dotenv()

# --- Graph Contruction ---

graph = StateGraph(IOState)

# Header Step
graph.add_node("check_user_intent", check_user_intent)
graph.add_node("get_attachments", get_attachments)
graph.add_node("process_multiple_attachments", process_multiple_attachments)
graph.add_node("download_and_convert_attachment", download_and_convert_attachment)
graph.add_node("extract_header", extract_header)
graph.add_node("validate_header", validate_header)
graph.add_node("retry_header", retry_header)

# Line Item Step
graph.add_node("extract_line_items", extract_line_items)
graph.add_node("validate_line_items", validate_line_items)
graph.add_node("retry_line_items", retry_line_items)
graph.add_node("similarity_analysis", similarity_analysis_json_builder)
graph.add_node("call_similarity_analysis", call_similarity_analysis)
graph.add_node("handle_user_selection_of_campaign", handle_user_selection_of_campaign)

# Account Matching Step
graph.add_node("get_account_soql", get_account_soql)
graph.add_node("account_similarity_analysis_json_builder", account_similarity_analysis_json_builder)
graph.add_node("call_account_similarity_analysis", call_account_similarity_analysis)

# Quote Matching Step
graph.add_node("generate_quote_soql", generate_quote_soql)
graph.add_node("validate_line_items_loop", validate_line_items_loop)

# Finalize
# Finalize
graph.add_node("finalize", finalize)
# graph.add_node("create_salesforce_payload", create_salesforce_payload)
# graph.add_node("insert_order_line_items", insert_order_line_items)

# Dynamic Payload Generation Nodes
graph.add_node("build_order_payload_agent", build_order_payload_agent)
graph.add_node("insert_order_mcp", insert_order_mcp)
graph.add_node("build_line_items_payload_agent", build_line_items_payload_agent)
graph.add_node("insert_line_items_mcp", insert_line_items_mcp)
graph.add_node("error_handler", error_handler)
graph.add_node("return_status_of_order_items", return_status_of_order_items)

# Header Edges
graph.add_edge(START, "check_user_intent")

def route_user_intent(state: IOState) -> str:
    # Hyper-Fast Jump: If we are resuming with a selection, bypass extraction
    logger.info(f"🔀 route_user_intent: user_selection={state.user_selection}, "
                f"has_matched_records={bool(state.matched_opportunity_records)}, "
                f"intent_valid={state.intent_valid}")
    if state.user_selection and state.matched_opportunity_records:
        logger.info("⚡ Resume detected. Skipping extraction and jumping to selection handler.")
        return "handle_user_selection_of_campaign"
    if state.intent_valid:
        return "get_attachments"
    return END

graph.add_conditional_edges("check_user_intent", route_user_intent, {
    "get_attachments": "get_attachments",
    "handle_user_selection_of_campaign": "handle_user_selection_of_campaign",
    END: END
})

def route_after_attachments(state: IOState) -> str:
    if state.awaiting_file_selection:
        return "process_multiple_attachments"
    return "download_and_convert_attachment"

graph.add_conditional_edges("get_attachments", route_after_attachments, {
    "process_multiple_attachments": "process_multiple_attachments",
    "download_and_convert_attachment": "download_and_convert_attachment"
})

graph.add_edge("process_multiple_attachments", END)
graph.add_edge("download_and_convert_attachment", "extract_header")
graph.add_edge("extract_header", "validate_header")

def route_after_header(state: IOState) -> str:
    if state.header_valid:
        return "extract_line_items"
    if state.header_attempt >= state.max_attempts:
        print("Max header attempts reached. Proceeding with potentially invalid header.")
        return "extract_line_items" # Or finalize, but let's try to get line items anyway
    return "retry_header"

graph.add_conditional_edges("validate_header", route_after_header, {
    "extract_line_items": "extract_line_items",
    "retry_header": "retry_header"
})
graph.add_edge("retry_header", "validate_header")
graph.add_edge("similarity_analysis","call_similarity_analysis")
graph.add_edge("similarity_analysis","call_similarity_analysis")

def route_after_similarity(state: IOState) -> str:
    if state.matched_opportunity_type == "multiple_perfect":
        return "handle_user_selection_of_campaign"
    return "get_account_soql"

graph.add_conditional_edges("call_similarity_analysis", route_after_similarity, {
    "handle_user_selection_of_campaign": "handle_user_selection_of_campaign",
    "get_account_soql": "get_account_soql"
})

def route_after_selection(state: IOState) -> str:
    if state.awaiting_selection:
        return END
    return "get_account_soql"

graph.add_conditional_edges("handle_user_selection_of_campaign", route_after_selection, {
    END: END,
    "get_account_soql": "get_account_soql"
})

# Account Edges
graph.add_edge("get_account_soql", "account_similarity_analysis_json_builder")
graph.add_edge("account_similarity_analysis_json_builder", "call_account_similarity_analysis")
graph.add_edge("call_account_similarity_analysis", "generate_quote_soql")

# Quote Edges
graph.add_edge("generate_quote_soql", "validate_line_items_loop")
graph.add_edge("validate_line_items_loop", "finalize")

# Line Item Edges
graph.add_edge("extract_line_items", "validate_line_items")


def route_after_line(state: IOState) -> str:
    if state.line_items_valid:
        return "finalize"
    if state.line_attempt >= state.max_attempts:
        print("Max line item attempts reached.")
        return "finalize"
    return "retry_line_items"

graph.add_conditional_edges("validate_line_items", route_after_line, {
    "finalize": "similarity_analysis",
    "retry_line_items": "retry_line_items"
})
graph.add_edge("retry_line_items", "validate_line_items")
#graph.add_edge("similarity_analysis", "finalize")
# graph.add_edge("finalize", "create_salesforce_payload")
# graph.add_edge("create_salesforce_payload", "insert_order_line_items")
# graph.add_edge("insert_order_line_items", END)

# Dynamic Payload Edges
graph.add_edge("finalize", "build_order_payload_agent")
graph.add_edge("build_order_payload_agent", "insert_order_mcp")

def route_after_order_insertion(state: IOState) -> str:
    if state.insertion_errors or state.error_flag:
        return "error_handler"
    return "build_line_items_payload_agent"

graph.add_conditional_edges("insert_order_mcp", route_after_order_insertion, {
    "error_handler": "error_handler",
    "build_line_items_payload_agent": "build_line_items_payload_agent"
})

graph.add_edge("build_line_items_payload_agent", "insert_line_items_mcp")

def route_after_line_item_insertion(state: IOState) -> str:
    if state.insertion_errors or state.error_flag:
        return "error_handler"
    return "return_status_of_order_items"

graph.add_conditional_edges("insert_line_items_mcp", route_after_line_item_insertion, {
    "error_handler": "error_handler",
    "return_status_of_order_items": "return_status_of_order_items"
})

graph.add_edge("error_handler", "return_status_of_order_items")
graph.add_edge("return_status_of_order_items", END)

# Compile Graph
io_agent = graph.compile()


# --- Main Execution ---
if __name__ == "__main__":
    # file_path = "/Users/bharat/Io_agent/Io_agent_v4/output8.md"
    # if not os.path.exists(file_path):
    #     print(f"Error: File {file_path} not found.")
    # else:
    #     with open(file_path, "r", encoding="utf-8") as f:
    #         content = f.read()

    print(f"Starting Agent processing with Case ID...")
    # initial_state = IOState(io_markdown=content)
    # Add user_input="start" to pass the intent check
    initial_state = IOState(case_id="500dN00000JmbqLQAR", user_input="start")
    
    # Run graph
    result = io_agent.invoke(initial_state)
        
    # Output Results
    print("\n\n=== Extraction Results ===")
    
    # Serialize result for display
    # Note: IOState is a Pydantic model, but invoke returns a dict-like state in some versions or the object.
    # LangGraph invoke typically returns the state dict.
    
    # Manual print for clarity
    if result.get("finalized_record"):
        print("Finalized Record:")
        print(json.dumps(result["finalized_record"].model_dump(), indent=2))
    
    if result.get("salesforce_payload"):
        print("\nSalesforce Payload:")
        print(json.dumps(result["salesforce_payload"], indent=2))

    # Use .get() to avoid KeyErrors if the graph exited early
    print(json.dumps({
        "media_company": result.get("media_company").model_dump() if result.get("media_company") else None,
        "client_agency": result.get("client_agency").model_dump() if result.get("client_agency") else None,
        "campaign_information": result.get("campaign_information").model_dump() if result.get("campaign_information") else None,
        "terms": result.get("terms").model_dump() if result.get("terms") else None,
        "io_id": result.get("io_id"),
        "order_id": result.get("order_id"),
        "best_matched_line_items": result.get("best_matched_line_items"),
        "line_items": [li.model_dump() for li in result.get("line_items", [])],
        "header_valid": result.get("header_valid"),
        "line_items_valid": result.get("line_items_valid"),
        "insertion_errors": result.get("insertion_errors")
    }, indent=2))
