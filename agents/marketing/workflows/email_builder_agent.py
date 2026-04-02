from langgraph.graph import StateGraph, END
from agents.marketing.state import MarketingState, EmailAgentState
from nodes.email_builder_node import email_builder_node

def build_email_builder_agent():
    """
    Constructs the Email Builder Agent graph.
    """
    builder = StateGraph(EmailAgentState)
    
    builder.add_node("email_builder_node", email_builder_node)
    
    builder.set_entry_point("email_builder_node")
    builder.add_edge("email_builder_node", END)
    
    return builder.compile()
