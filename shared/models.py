"""Shared models for agent communication"""
from pydantic import BaseModel
from typing import Dict, Any, Optional, List


class AgentRequest(BaseModel):
    """Standard request format for all agents"""
    user_goal: str
    context: Dict[str, Any] = {}
    session_id: str
    messages: List[Dict[str, Any]] = []
    resume: Optional[Any] = None  # Payload to resume execution after interrupt
    

class AgentResponse(BaseModel):
    """Standard response format from all agents"""
    status: str  # "success", "error", "interrupted"
    result: Dict[str, Any]
    next_action: Optional[str] = None
    error: Optional[str] = None
    final_response: Optional[str] = None
    created_records: Optional[Dict[str, Any]] = None
    generated_email_content: Optional[Any] = None
