from pacepal.state import PacepalState
from langgraph.graph import END
import logging

logger = logging.getLogger(__name__)

def route_orchestrator(state: PacepalState) -> str:
    """Route based on selected agent"""
    agent = state.get("selected_agent")
    
    if agent:
        # Route to the dynamic agent caller
        return "agent_caller"
    
    logger.warning(f"⚠️ No valid agent selected (got: {agent}), ending workflow")
    return END
