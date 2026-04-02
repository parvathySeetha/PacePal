# agents/marketing/state.py
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from core.state import merge_dicts, merge_history

class MarketingState(TypedDict):
    user_goal: str
    messages: Annotated[List[BaseMessage], add_messages]
    
    # Session tracking
    session_id: Optional[str]
    resume_data: Optional[Any]  # Set when resuming from interrupt via Command(resume=...)
    
    # Orchestrator routing
    iteration_count: int
    max_iterations: int
    next_action: str  # "salesforce" | "brevo" | "linkly" | "complete"
    current_agent: str
    
    # Data coming back from sub-agents - use merge_dicts to preserve across updates
    # Data coming back from sub-agents - use merge_dicts to preserve across updates
    salesforce_data: Annotated[Optional[Dict[str, Any]], merge_dicts]
    brevo_results: Annotated[Optional[Dict[str, Any]], merge_dicts]
    linkly_links: Annotated[Optional[Dict[str, Any]], merge_dicts]
    
    # Generic MCP results storage for dynamic handling
    # ✅ Re-added merge_dicts to ensure results persist across turns even if context is incomplete
    mcp_results: Annotated[Optional[Dict[str, Any]], merge_dicts]
    
    # Persistent Session History
    session_history: Annotated[Optional[List[Dict[str, Any]]], merge_history]
    
    # 🔑 Session Context: Stores created records across multiple requests in same WebSocket session
    session_context: Annotated[Optional[Dict[str, Any]], lambda x, y: y if y is not None else x]
      
    # ✅ SHARED RESULT SETS: Data persistence across agents (e.g. campaign data for Brevo)
    # List-based accumulation: Each operation appends a new dict, latest item is current context
    # Reducer only appends non-empty dicts to prevent pollution
    shared_result_sets: Annotated[Optional[List[Dict[str, Any]]], lambda x, y: (x or []) + [y] if (y is not None and isinstance(y, dict) and y) else (x or [])]
    
    # ✅ TASK DIRECTIVES: For multi-step workflows (e.g. update CampaignMember status after email send)
    task_directive: Optional[str]
    pending_updates: Optional[Dict[str, Any]]

    # 🔗 CREATED RECORDS: For LWC hyperlink generation (extracted by completion node)
    created_records: Optional[Dict[str, Any]]

    # ✅ EMAIL WORKFLOW CONTEXT: Temporary state for deterministic email workflow
    email_workflow_context: Optional[Dict[str, Any]]

    engagement_workflow_context: Optional[Dict[str, Any]]

    # Final result + errors
    error: Optional[str]
    final_response: Optional[str]
    
    # ✅ FAIL FLAG (To stop loops)
    workflow_failed: Optional[bool]
    
    # ✅ EMAIL BUILDER CONTENT
    # Removed merge_dicts to allow explicit clearing (setting to None)
    generated_email_content: Optional[Dict[str, Any]]
    
    # ✅ SAVE TEMPLATE WORKFLOW CONTEXT
    save_workflow_context: Optional[Dict[str, Any]]
    
    # 🔄 ACTIVE WORKFLOW (For Sticky Routing)
    # If set, bypasses orchestrator and goes directly to this agent/node
    active_workflow: Optional[str]

    # ✅ REVIEW PROPOSAL STATE
    # Used to resume execution after interrupt
    # 🛑 PROPOSAL REVIEW: Temporary storage for proposal under review
    pending_proposal_plan: Optional[Dict[str, Any]]
    pending_proposal_details: Optional[Dict[str, Any]]
    pending_proposal_result_sets: Optional[Dict[str, Any]]  # Current operation's result_sets for proposal UI
    
    # 🔄 PLAN OVERRIDE: User-approved plan to execute (bypasses planning):
    plan_override: Optional[Dict[str, Any]]
    
    # 🔗 CHAIN COMMAND: Stores the new user command when an interrupt is chained
    chain_command: Optional[str]

    # ✅ STRUCTURED SUMMARY
    structured_summary: Optional[Dict[str, Any]]


class EmailAgentState(TypedDict):
    """
    State optimized for the Email Builder Agent.
    Sub-set or compatible with MarketingState for easy handoff.
    """
    user_goal: str
    messages: Annotated[List[BaseMessage], add_messages]
    
    # Context needed for drafting
    session_context: Optional[Dict[str, Any]]
    
    # Output
    generated_email_content: Dict[str, Any]
    final_response: Optional[str]
    error: Optional[str]
    
    # 🔄 ACTIVE WORKFLOW (For Sticky Routing)
    active_workflow: Optional[str]
    
    # Routing
    next_action: Optional[str]
