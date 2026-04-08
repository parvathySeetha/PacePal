import logging
from langgraph.graph import StateGraph, END
from agents.Reconciliation.state import ReconcillationState
from agents.Reconciliation.nodes.fetch_delivery_data import fetch_delivery_data_node
from agents.Reconciliation.nodes.calculate import calculate_node
from agents.Reconciliation.nodes.amendment import amendment_node
from agents.Reconciliation.nodes.variance import variance_node
from agents.Reconciliation.nodes.summary_response import summary_response_node

logger = logging.getLogger(__name__)

def should_continue(state: ReconcillationState) -> str:
    if state.get("error"):
        logger.warning(f"🚨 Error detected in graph state: {state['error']}. Routing to summary.")
        return "summary"
    return "continue"


def build_reconcillation_graph(checkpointer=None):
    workflow = StateGraph(ReconcillationState)

    workflow.add_node("fetchdeliverydata", fetch_delivery_data_node)
    workflow.add_node("Calculate", calculate_node)
    workflow.add_node("Amendment", amendment_node)
    workflow.add_node("Variance", variance_node)
    workflow.add_node("summaryresponse", summary_response_node)

    workflow.set_entry_point("fetchdeliverydata")

    workflow.add_conditional_edges(
        "fetchdeliverydata",
        should_continue,
        {
            "continue": "Calculate",
            "summary": "summaryresponse"
        }
    )

    workflow.add_conditional_edges(
        "Calculate",
        should_continue,
        {
            "continue": "Amendment",
            "summary": "summaryresponse"
        }
    )

    workflow.add_conditional_edges(
        "Amendment",
        should_continue,
        {
            "continue": "Variance",
            "summary": "summaryresponse"
        }
    )

    workflow.add_edge("Variance", "summaryresponse")
    workflow.add_edge("summaryresponse", END)

    return workflow.compile(checkpointer=checkpointer)