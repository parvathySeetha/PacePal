import logging
from decimal import Decimal
from agents.Reconciliation.state import ReconcillationState
from agents.Reconciliation.nodes.utils import _to_decimal

logger = logging.getLogger(__name__)

async def variance_node(state: ReconcillationState) -> ReconcillationState:
    logger.info("📉 [Reconciliation] Calculating overall invoice variance...")

    results = state.get("amendment_results", {})
    correct = _to_decimal(results.get("calculated_total_revenue"))
    billed = _to_decimal(results.get("billed_total_revenue"))
    variance = correct - billed

    tolerance = max(Decimal("0.01"), abs(billed) * Decimal("0.00001"))

    if abs(variance) < Decimal("0.005"):
        variance = Decimal("0.00")

    state["variance_results"] = {
        "variance": variance,
        "status": "Underbilled" if variance > tolerance else "Overbilled" if variance < -tolerance else "Ok",
        "leakage_detected": variance > tolerance
    }

    return state
