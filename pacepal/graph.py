"""
PacePal Orchestrator - Top-level agent router
Supports both local (monolith) and remote (distributed) agent execution
"""
from typing import TypedDict, Optional, Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
import logging

from pacepal.state import PacepalState
from pacepal.nodes.PacepalOrchestrator import pacepal_orchestrator
from pacepal.nodes.agent_caller import call_dynamic_agent
from pacepal.nodes.router_node import route_orchestrator
from shared.config import DEPLOYMENT_MODE

logger = logging.getLogger(__name__)

def build_pacepal_graph(checkpointer: BaseCheckpointSaver = None):
    """
    Build the PacePal orchestrator graph.
    Uses a dynamic agent caller to execute agents based on registry endpoints.
    """
    builder = StateGraph(PacepalState)
    
    # Add orchestrator node
    builder.add_node("pacepal_orchestrator", pacepal_orchestrator)
    
    # Add dynamic agent caller (handles remote dispatch via registry URLs)
    builder.add_node("agent_caller", call_dynamic_agent)
    
    # Set entry point
    builder.set_entry_point("pacepal_orchestrator")
    
    # Add conditional routing
    builder.add_conditional_edges(
        "pacepal_orchestrator",
        route_orchestrator,
        {
            "agent_caller": "agent_caller",
            END: END
        }
    )
    
    # Dynamic agent caller goes to END
    builder.add_edge("agent_caller", END)
    
    logger.info(f"✅ PacePal graph built (mode: {DEPLOYMENT_MODE})")
    
    return builder.compile(checkpointer=checkpointer)
