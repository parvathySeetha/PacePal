import logging
from decimal import Decimal
from agents.Reconciliation.state import ReconcillationState
from agents.Reconciliation.nodes.utils import _to_decimal

logger = logging.getLogger(__name__)

async def calculate_node(state: ReconcillationState) -> ReconcillationState:
    """
    Calculate grouped metrics.
    IMPORTANT: revenue is calculated using actual row-level OrderItem rate.
    """
    logger.info("📊 [Reconciliation] Calculating grouped line metrics...")

    data = state.get("delivery_data", {})
    line_items = data.get("line_items", [])

    total_metrics = {
        "total_gross": Decimal("0.0"),
        "total_invalid": Decimal("0.0"),
        "total_valid": Decimal("0.0"),
        "total_billable_viewable": Decimal("0.0"),
        "line_metrics": []
    }

    for item in line_items:
        blocks = item.get("daily_blocks", [])

        gross = Decimal("0.0")
        invalid = Decimal("0.0")
        valid_impressions = Decimal("0.0")
        billable_viewable = Decimal("0.0")
        calculated_revenue = Decimal("0.0")

        for b in blocks:
            row_gross = _to_decimal(b.get("gross"))
            row_ivt = _to_decimal(b.get("ivt"))
            row_viewability = _to_decimal(b.get("viewability"))
            row_rate = b.get("orderLineRate") or _to_decimal(item.get("effective_rate"))
            row_pricing_model = b.get("orderLinePricingModel") or item.get("pricing_model", "CPM")

            row_invalid = row_gross * row_ivt
            row_valid = row_gross - row_invalid
            row_billable_viewable = row_valid * row_viewability

            if row_pricing_model == "CPM":
                row_revenue = (row_billable_viewable / Decimal("1000")) * row_rate
            else:
                row_revenue = row_billable_viewable * row_rate

            gross += row_gross
            invalid += row_invalid
            valid_impressions += row_valid
            billable_viewable += row_billable_viewable
            calculated_revenue += row_revenue

        ivt_pct = (invalid / gross) if gross > 0 else Decimal("0")
        viewability_pct = (billable_viewable / valid_impressions) if valid_impressions > 0 else Decimal("0")

        item_metrics = {
            "ili_id": item.get("ili_id"),
            "ili_name": item.get("ili_name"),
            "product_id": item.get("product_id"),
            "oli_ids": item.get("oli_ids", []),
            "oli_names": item.get("oli_names", []),
            "effective_rate": _to_decimal(item.get("effective_rate")),
            "pricing_model": item.get("pricing_model", "CPM"),
            "billed_impressions": _to_decimal(item.get("billed_impressions")),
            "billed_amount": _to_decimal(item.get("billed_amount")),
            "gross": gross,
            "invalid": invalid,
            "ivt_pct": ivt_pct,
            "viewability_pct": viewability_pct,
            "valid_impressions": valid_impressions,
            "billable_viewable": billable_viewable,
            "calculated_impressions": billable_viewable,
            "calculated_revenue": calculated_revenue,
        }

        total_metrics["line_metrics"].append(item_metrics)
        total_metrics["total_gross"] += gross
        total_metrics["total_invalid"] += invalid
        total_metrics["total_valid"] += valid_impressions
        total_metrics["total_billable_viewable"] += billable_viewable

    total_metrics["avg_ivt_pct"] = (
        total_metrics["total_invalid"] / total_metrics["total_gross"]
        if total_metrics["total_gross"] > 0 else Decimal("0")
    )

    total_metrics["avg_viewability_pct"] = (
        total_metrics["total_billable_viewable"] / total_metrics["total_valid"]
        if total_metrics["total_valid"] > 0 else Decimal("0")
    )

    state["monthly_metrics"] = total_metrics
    logger.info(f"✅ Grouped monthly metrics calculated: {total_metrics}")
    return state
