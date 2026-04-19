import logging
from decimal import Decimal
from agents.Reconciliation.state import ReconcillationState
from core.helper import call_llm, fetch_prompt_metadata, resolve_placeholders
from agents.Reconciliation.nodes.utils import _to_float, _to_decimal

logger = logging.getLogger(__name__)

async def summary_response_node(state: ReconcillationState) -> ReconcillationState:
    logger.info("💬 [Reconciliation] Generating summary response...")

    error = state.get("error")
    user_goal = state.get("user_goal", "")
    variance = state.get("variance_results", {})
    metrics = state.get("monthly_metrics", {})
    amendment = state.get("amendment_results", {})

    system_prompt = None
    user_prompt = None

    # try:
    #     prompt_meta = fetch_prompt_metadata("summary_response_node", "Reconciliation Agent")
    #
    #     if prompt_meta and prompt_meta.get("prompt"):
    #         system_prompt = resolve_placeholders(
    #             prompt_meta["prompt"],
    #             prompt_meta.get("configs", {}),
    #             state
    #         )
    #         user_prompt = ""
    #         logger.info("✅ Using prompt metadata for summary_response_node")
    #     else:
    #         logger.warning("⚠️ Prompt metadata not found. Using fallback prompt.")
    #
    # except Exception as e:
    #     logger.warning(f"⚠️ Failed to fetch/resolve prompt metadata. Using fallback prompt. Error: {e}")

    if not system_prompt:
        if error:
            system_prompt = (
                "You are a specialized Reconciliation Agent. Briefly explain why the reconciliation could not be completed."
            )
            user_prompt = (
                f"User Question: {user_goal}\n"
                f"Error Encountered: {error}\n\n"
                f"Please explain to the user that we couldn't complete the validation and why."
            )
        else:
            system_prompt = (
                "You are a specialized Reconciliation Agent. Provide a very brief summary of the invoice vs expected delivery. "
                "Do NOT explain line-by-line math. Only tell the user whether the invoice is good to proceed, Underbilled, or Overbilled, "
                "and direct them to click 'View Details' for the full breakdown."
            )
            user_prompt = f"""
User Question: {user_goal}

Reconciliation Data:
- Total Gross Impressions: {metrics.get('total_gross', 0):,.0f}
- Total Valid Impressions: {metrics.get('total_valid', 0):,.0f}
- Total Billable Viewable Impressions: {metrics.get('total_billable_viewable', 0):,.0f}
- Billed Revenue (Invoice): ₹{amendment.get('billed_total_revenue', 0):,.2f}
- Calculated Valid Revenue: ₹{amendment.get('calculated_total_revenue', 0):,.2f}
- Variance: ₹{variance.get('variance', 0):,.2f}
- Status: {variance.get('status', 'Unknown')}
"""
        logger.info("✅ Using fallback prompt for summary_response_node")

    try:
        # PS1904 COMMNTED
        # response = await call_llm(
        #     system_prompt=system_prompt,
        #     user_prompt=user_prompt,
        #     default_model="gpt-4o",
        #     default_provider="openai",
        #     default_temperature=0
        # )

        response = await call_llm(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            default_model="gemma-4-26b-a4b-it",
            default_provider="gemini",
            default_temperature=0
        )

        state["final_response"] = response.strip()

        status_raw = variance.get("status", "Unknown")
        variant = "success" if status_raw == "Ok" else "error"
        line_items_card = []

        for idx, res in enumerate(amendment.get("line_results", []), start=1):
            ili_name = res.get("ili_name")
            product_id = res.get("product_id")
            effective_rate = _to_float(res.get("effective_rate"))
            date_str = "N/A"
            daily_blocks_enriched = []
            billed_impressions = 0.0
            period_start = "N/A"
            period_end = "N/A"
            oli_names = res.get("oli_names", [])

            for item in state.get("delivery_data", {}).get("line_items", []):
                if item.get("product_id") == product_id:
                    billed_impressions = _to_float(item.get("billed_impressions"))
                    period_start = item.get("period_start", "N/A")
                    period_end = item.get("period_end", "N/A")

                    dates = [b.get("date") for b in item.get("daily_blocks", []) if b.get("date")]
                    if dates:
                        date_str = f"{min(dates)} to {max(dates)}"

                    for b in item.get("daily_blocks", []):
                        gross = _to_float(b.get("gross"))
                        ivt_pct = _to_float(b.get("ivt"))
                        viewability_pct = _to_float(b.get("viewability"))
                        row_rate = _to_float(b.get("orderLineRate"), effective_rate)
                        row_pricing_model = b.get("orderLinePricingModel") or item.get("pricing_model", "CPM")

                        invalid_impressions = gross * ivt_pct
                        valid_impressions = gross - invalid_impressions
                        viewable_impressions = valid_impressions * viewability_pct

                        if row_pricing_model == "CPM":
                            daily_revenue = (viewable_impressions / 1000) * row_rate
                        else:
                            daily_revenue = viewable_impressions * row_rate

                        daily_blocks_enriched.append({
                            "date": b.get("date"),
                            "gross": gross,
                            "ivtPct": ivt_pct * 100,
                            "viewabilityPct": viewability_pct * 100,
                            "invalidImpressions": invalid_impressions,
                            "validImpressions": valid_impressions,
                            "viewableImpressions": viewable_impressions,
                            "orderLineItem": b.get("orderLineItemNumber") or "N/A",
                            "pricingModel": row_pricing_model,
                            "rate": row_rate,
                            "revenue": daily_revenue,
                            "productName": ili_name,
                            "description": f"Billable Viewable: {viewable_impressions:,.3f}, Revenue: ₹{daily_revenue:,.2f} (Rate: ₹{row_rate:,.4f})"
                        })
                    break

            line_variance = _to_decimal(res.get("variance"))
            gross_total = sum(_to_decimal(b["gross"]) for b in daily_blocks_enriched)
            invalid_total = sum(_to_decimal(b["invalidImpressions"]) for b in daily_blocks_enriched)
            valid_total = sum(_to_decimal(b["validImpressions"]) for b in daily_blocks_enriched)
            viewable_total = sum(_to_decimal(b["viewableImpressions"]) for b in daily_blocks_enriched)
            calculated_revenue = _to_decimal(res.get("calculated_revenue"))
            billed_revenue = _to_decimal(res.get("billed_revenue"))

            segment_map = {}
            for b in daily_blocks_enriched:
                oli_num = b.get("orderLineItem") or "N/A"
                seg = segment_map.setdefault(oli_num, {
                    "rate": _to_decimal(b.get("rate")),
                    "pricingModel": b.get("pricingModel"),
                    "gross": Decimal("0.0"),
                    "invalid": Decimal("0.0"),
                    "valid": Decimal("0.0"),
                    "viewable": Decimal("0.0"),
                    "revenue": Decimal("0.0"),
                    "dates": []
                })
                seg["gross"] += _to_decimal(b.get("gross"))
                seg["invalid"] += _to_decimal(b.get("invalidImpressions"))
                seg["valid"] += _to_decimal(b.get("validImpressions"))
                seg["viewable"] += _to_decimal(b.get("viewableImpressions"))
                seg["dates"].append(b.get("date"))

                if seg["pricingModel"] == "CPM":
                    seg["revenue"] += (_to_decimal(b.get("viewableImpressions")) / Decimal("1000")) * _to_decimal(b.get("rate"))
                else:
                    seg["revenue"] += _to_decimal(b.get("viewableImpressions")) * _to_decimal(b.get("rate"))

            segment_lines = []
            for oli_num, seg in segment_map.items():
                seg_start = min([d for d in seg["dates"] if d]) if seg["dates"] else "N/A"
                seg_end = max([d for d in seg["dates"] if d]) if seg["dates"] else "N/A"
                segment_lines.append(
                    f"{oli_num}: {seg_start} to {seg_end} at ₹{seg['rate']:,.2f} {seg['pricingModel']} → Revenue ₹{seg['revenue']:,.2f}"
                )

            segment_text = "\n".join(segment_lines) if segment_lines else "No amendment segments found."

            billed_imps_val = _to_decimal(res.get("billed_impressions"))
            calc_imps_val = _to_decimal(res.get("calculated_impressions"))
            imp_diff_pct = abs(billed_imps_val - calc_imps_val) / billed_imps_val if billed_imps_val > 0 else Decimal("0")
            is_rate_mismatch = imp_diff_pct < Decimal("0.0001") and abs(line_variance) > Decimal("0.01")

            if abs(line_variance) < Decimal("0.01"):
                description = (
                    f"Status: Ok.\n\n"
                    f"Gross Impressions: {gross_total:,.0f}\n"
                    f"Invalid Traffic Removed: {invalid_total:,.0f}\n"
                    f"Valid Impressions: {valid_total:,.0f}\n"
                    f"Billable Viewable Impressions: {viewable_total:,.3f}\n\n"
                    f"Effective CPM: ₹{effective_rate:,.4f} {res.get('pricing_model', 'CPM')}\n"
                    f"Calculated Revenue: ₹{calculated_revenue:,.2f}\n"
                    f"Billed Revenue: ₹{billed_revenue:,.2f}\n"
                    f"Variance: ₹{line_variance:,.2f}\n\n"
                    f"Amendment Breakdown:\n{segment_text}\n\n"
                    f"This grouped product line is ok because the billed amount exactly matches the revenue derived "
                    f"from verified delivery data after IVT and viewability adjustments."
                )
            elif line_variance > 0:
                rate_reason = " (primarily due to a rate mismatch)" if is_rate_mismatch else ""
                description = (
                    f"Status: Underbilled.\n\n"
                    f"Gross Impressions: {gross_total:,.0f}\n"
                    f"Invalid Traffic Removed: {invalid_total:,.0f}\n"
                    f"Valid Impressions: {valid_total:,.0f}\n"
                    f"Billable Viewable Impressions: {viewable_total:,.3f}\n\n"
                    f"Effective CPM: ₹{effective_rate:,.4f} {res.get('pricing_model', 'CPM')}\n"
                    f"Calculated Revenue: ₹{calculated_revenue:,.2f}\n"
                    f"Billed Revenue: ₹{billed_revenue:,.2f}\n"
                    f"Variance: ₹{line_variance:,.2f}\n\n"
                    f"Amendment Breakdown:\n{segment_text}\n\n"
                    f"This grouped product line is underbilled{rate_reason} because the delivery-derived revenue is higher than the billed amount."
                )
            else:
                rate_reason = " (primarily due to a rate mismatch)" if is_rate_mismatch else ""
                description = (
                    f"Status: Overbilled.\n\n"
                    f"Gross Impressions: {gross_total:,.0f}\n"
                    f"Invalid Traffic Removed: {invalid_total:,.0f}\n"
                    f"Valid Impressions: {valid_total:,.0f}\n"
                    f"Billable Viewable Impressions: {viewable_total:,.3f}\n\n"
                    f"Effective CPM: ₹{effective_rate:,.4f} {res.get('pricing_model', 'CPM')}\n"
                    f"Calculated Revenue: ₹{calculated_revenue:,.2f}\n"
                    f"Billed Revenue: ₹{billed_revenue:,.2f}\n"
                    f"Variance: ₹{line_variance:,.2f}\n\n"
                    f"Amendment Breakdown:\n{segment_text}\n\n"
                    f"This grouped product line is overbilled{rate_reason} because the billed amount is higher than the revenue "
                    f"derived from verified delivery data after IVT and viewability adjustments."
                )

            line_items_card.append({
                "lineNumber": idx,
                "name": ili_name,
                "effectiveRate": _to_float(effective_rate),
                "rate": _to_float(effective_rate),
                "dates": date_str,
                "periodStart": period_start,
                "periodEnd": period_end,
                "revenue": _to_float(calculated_revenue),
                "billedRevenue": _to_float(billed_revenue),
                "billedImpressions": _to_float(billed_impressions),
                "calculatedImpressions": _to_float(res.get("calculated_impressions")),
                "pricingModel": res.get("pricing_model"),
                "status": res.get("status"),
                "orderLineItems": oli_names,
                "grossImpressions": _to_float(gross_total),
                "invalidImpressions": _to_float(invalid_total),
                "validImpressions": _to_float(valid_total),
                "viewableImpressions": _to_float(viewable_total),
                "dailyBlocks": daily_blocks_enriched,
                "description": description
            })

        state["structured_summary"] = {
            "status": status_raw,
            "statusVariant": variant,
            "currencyCode": "INR",
            "currencySymbol": "₹",
            "totalImpressions": _to_float(state.get("monthly_metrics", {}).get("total_gross")),
            "totalValidImpressions": _to_float(state.get("monthly_metrics", {}).get("total_valid")),
            "totalViewableImpressions": _to_float(state.get("monthly_metrics", {}).get("total_billable_viewable")),
            "totalRevenue": _to_float(amendment.get("calculated_total_revenue")),
            "totalBilled": _to_float(amendment.get("billed_total_revenue")),
            "variance": _to_float(variance.get("variance")),
            "lineItems": line_items_card,
            "invoiceId": state.get("invoice_data", {}).get("id", "Unknown"),
            "invoiceName": state.get("invoice_data", {}).get("name", "Unknown"),
            "invoiceDate": state.get("invoice_data", {}).get("invoice_date"),
            "invoiceStartDate": state.get("invoice_data", {}).get("start"),
            "invoiceEndDate": state.get("invoice_data", {}).get("end"),
            "advertiserName": state.get("invoice_data", {}).get("advertiser_name", "Unknown"),
            "advertiserId": state.get("invoice_data", {}).get("advertiser_id"),
            "orderId": state.get("record_id", "Unknown")
        }

        logger.info(f"✅ Structured summary populated: {variant}")

    except Exception as e:
        logger.error(f"Error in summary generation: {e}")
        if error:
            state["final_response"] = f"Technical Error: {error}. I was unable to complete the reconciliation process."
        else:
            state["final_response"] = (
                f"Reconciliation Complete. Status: {variance.get('status', 'Unknown')}. "
                f"Variance: ₹{_to_float(variance.get('variance')):,.2f}."
            )

    state["next_action"] = "complete"
    return state
