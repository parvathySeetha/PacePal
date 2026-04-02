from agents.marketing.state import MarketingState
from langgraph.graph import StateGraph, END
from nodes.marketingorchestrator import marketing_orchestrator
from nodes.dynamic_caller import dynamic_caller, review_proposal_node
from nodes.completion import completion_node
from workflows.engagement_workflow import build_engagement_workflow
from workflows.email_workflow import build_email_workflow
from workflows.email_builder_agent import build_email_builder_agent
import logging
 
def route_decision(state: MarketingState) -> str:
    """
    Router for the 'marketing_orchestrator' node.

    - If the marketing_orchestrator decided we're done (next_action == 'complete'),
      go to 'completion'.
    - If next_action is 'Brevomcp' (or 'EmailWorkflow'), route to specific email workflow.
    - If next_action is 'EngagementWorkflow', route to engagement workflow.
    - Otherwise, go to generic 'dynamic_caller'.
    """
    next_action = state.get("next_action", "complete")
    
    if next_action == "complete":
        return "complete"
    
    # 🚀 INTERCEPT Brevo calls to use the specialized workflow
    # 🚀 INTERCEPT Brevo calls to use the specialized workflow
    if next_action == "Brevo MCP":
        # Check if the intent is actually to SAVE a template (which is also a Brevo action)
        user_goal = state.get("user_goal", "").lower()
        if "save" in user_goal and "template" in user_goal:
            logging.info("🔀 Routing to Save Template Workflow (detected via intent)")
            return "save_template_workflow"
            
        logging.info("🔀 Routing to deterministic Email Workflow")
        return "email_workflow"
        
    if next_action == "EngagementWorkflow":
        logging.info("🔀 Routing to deterministic Engagement Workflow")
        return "engagement_workflow"
        
    if next_action in ["EmailBuilderAgent", "Email Builder Agent"]:
        logging.info("🔀 Routing to Email Builder Agent")
        return "email_builder_agent"

    return "dynamic_caller"
from workflows.save_template_workflow import build_save_template_workflow

def route_builder(state: MarketingState) -> str:
    """
    Router for Email Builder Agent.
    - If next_action is 'save_template', transition to save workflow.
    - Otherwise, END (wait for user input).
    """
    if state.get("next_action") == "save_template":
        logging.info("🔀 [BuilderRouter] Transitioning to Save Template Workflow")
        return "save_template_workflow"
    return END

def start_router(state: MarketingState) -> str:
    """
    Determines the entry point.
    - If active_workflow is set, bypass orchestrator.
    - Otherwise, go to orchestrator.
    """
    active = state.get("active_workflow")
    if active in ["email_builder_agent", "Email Builder Agent"]:
        logging.info(f"🔀 [StartRouter] Resuming active workflow: {active}")
        return "email_builder_agent"
    
    if active == "save_template_workflow":
        logging.info(f"🔀 [StartRouter] Resuming active workflow: {active}")
        return "save_template_workflow"
    
    return "marketing_orchestrator"

def build_marketing_graph(checkpointer=None):
    builder = StateGraph(MarketingState)

    builder.add_node("marketing_orchestrator", marketing_orchestrator)
    builder.add_node("dynamic_caller", dynamic_caller)
    builder.add_node("review_proposal", review_proposal_node)
    builder.add_node("completion", completion_node)
    
    # ✅ Add Subgraphs
    builder.add_node("email_workflow", build_email_workflow())
    builder.add_node("engagement_workflow", build_engagement_workflow())
    builder.add_node("email_builder_agent", build_email_builder_agent())
    builder.add_node("save_template_workflow", build_save_template_workflow())

    # ✅ Conditional Entry Point
    builder.set_conditional_entry_point(
        start_router,
        {
            "marketing_orchestrator": "marketing_orchestrator",
            "email_builder_agent": "email_builder_agent",
            "save_template_workflow": "save_template_workflow"
        }
    )
    
    builder.add_conditional_edges(
        "marketing_orchestrator",
        route_decision,
        {
            "dynamic_caller": "dynamic_caller",
            "email_workflow": "email_workflow",
            "engagement_workflow": "engagement_workflow",
            "email_builder_agent": "email_builder_agent",
            "save_template_workflow": "save_template_workflow",
            "complete": "completion",
        },
    )
    

    # ✅ CONDITIONAL LOOP: Check if email workflow failed
    def should_continue(state):
        if state.get("workflow_failed"):
            logging.info("🛑 Workflow marked as failed. Stopping execution.")
            return END
        return "marketing_orchestrator"

    builder.add_conditional_edges(
        "email_workflow",
        should_continue,
        {
            "marketing_orchestrator": "marketing_orchestrator",
            END: END
        }
    )
    builder.add_conditional_edges(
        "engagement_workflow",
        should_continue,
        {
            "marketing_orchestrator": "marketing_orchestrator",
            END: END
        }
    )
    
    # ✅ Email Builder Transition Logic
    builder.add_conditional_edges(
        "email_builder_agent",
        route_builder,
        {
            "save_template_workflow": "save_template_workflow",
            END: END
        }
    )
    
    # Save Workflow loops to END to ensure the response is sent and we exit cleanly
    builder.add_edge("save_template_workflow", END)

    builder.add_edge("completion", END)
    
    return builder.compile(checkpointer=checkpointer)
