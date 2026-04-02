# agents/reconcillationagent/state.py
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from core.state import merge_dicts, merge_history

class ReconcillationState(TypedDict):
    user_goal: str
    messages: Annotated[List[BaseMessage], add_messages]
    
    # Session tracking
    session_id: Optional[str]
    record_id: Optional[str]  # Record context from LWC
    resume_data: Optional[Any]
    
    # Orchestrator routing
    iteration_count: int
    max_iterations: int
    next_action: str
    current_agent: str
    
    # Data results
    salesforce_data: Annotated[Optional[Dict[str, Any]], merge_dicts]
    mcp_results: Annotated[Optional[Dict[str, Any]], merge_dicts]
    shared_result_sets: Annotated[Optional[List[Dict[str, Any]]], lambda x, y: (x or []) + [y] if (y is not None and isinstance(y, dict) and y) else (x or [])]
    
    # Session context
    session_context: Annotated[Optional[Dict[str, Any]], lambda x, y: y if y is not None else x]
    
    # Final result + errors
    error: Optional[str]
    final_response: Optional[str]
    workflow_failed: Optional[bool]
    
    # Chain command for orchestrator
    chain_command: Optional[str]
    
    # Demo Specific Fields
    delivery_data: Optional[Dict[str, Any]]
    monthly_metrics: Optional[Dict[str, Any]]
    amendment_results: Optional[Dict[str, Any]]
    variance_results: Optional[Dict[str, Any]]
    invoice_data: Optional[Dict[str, Any]]
    structured_summary: Optional[Dict[str, Any]]
