import logging
from decimal import Decimal
from agents.Reconciliation.state import ReconcillationState
from agents.Reconciliation.nodes.utils import _to_decimal

logger = logging.getLogger(__name__)

async def amendment_node(state: ReconcillationState) -> ReconcillationState:
    """
    Compare grouped billed amount vs grouped calculated revenue.
    """
    logger.info("📑 [Reconciliation] Checking grouped invoice variance vs delivery...")

    metrics = state.get("monthly_metrics", {})
    delivery_data = state.get("delivery_data", {})
    line_results = []

    total_calculated_revenue = Decimal("0.0")
    total_billed_revenue = Decimal("0.0")

    for item_metrics, item_raw in zip(metrics.get("line_metrics", []), delivery_data.get("line_items", [])):
        calculated_imp = _to_decimal(item_metrics.get("calculated_impressions"))
        calculated_revenue = _to_decimal(item_metrics.get("calculated_revenue"))
        billed_revenue = _to_decimal(item_metrics.get("billed_amount"))
        variance = calculated_revenue - billed_revenue

        tolerance = max(Decimal("0.01"), abs(billed_revenue) * Decimal("0.00001"))

        status = "Ok"
        if variance > tolerance:
            status = "Underbilled (Potential Leakage)"
        elif variance < -tolerance:
            status = "Overbilled (Customer Disputed)"

        line_results.append({
            "ili_name": item_metrics.get("ili_name"),
            "product_id": item_raw.get("product_id"),
            "oli_ids": item_raw.get("oli_ids", []),
            "oli_names": item_raw.get("oli_names", []),
            "pricing_model": item_raw.get("pricing_model", "CPM"),
            "effective_rate": _to_decimal(item_raw.get("effective_rate")),
            "calculated_impressions": calculated_imp,
            "billed_impressions": _to_decimal(item_metrics.get("billed_impressions")),
            "calculated_revenue": calculated_revenue,
            "billed_revenue": billed_revenue,
            "variance": variance,
            "status": status
        })

        total_calculated_revenue += calculated_revenue
        total_billed_revenue += billed_revenue

    state["amendment_results"] = {
        "line_results": line_results,
        "calculated_total_revenue": total_calculated_revenue,
        "billed_total_revenue": total_billed_revenue
    }

    return state
