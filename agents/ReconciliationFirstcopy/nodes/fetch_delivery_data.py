import logging
from decimal import Decimal
from agents.Reconciliation.state import ReconcillationState
from core.helper import ensure_sf_connected
from agents.Reconciliation.nodes.utils import _to_decimal, _safe_in_clause, sf_client

logger = logging.getLogger(__name__)

async def fetch_delivery_data_node(state: ReconcillationState) -> ReconcillationState:
    """
    Fetch grouped custom invoice lines (1 line per product),
    fetch matching OrderItems for that product under the order,
    and fetch delivery data within the invoice line period.

    Assumption:
    Advertiser name = Billing Account name on Custom_Invoice__c.
    """
    order_id = state.get("record_id")

    logger.info(f"PS order_id: {order_id}")


    if not order_id:
        logger.error("❌ No record_id found in state. Cannot fetch data.")
        state["error"] = "No Order ID provided context."
        return state


    logger.info(f"🔍 [Reconciliation] Fetching grouped Custom Invoice + Line Items for Order: '{order_id}'...")

    delivery_info = {
        "order_id": order_id,
        "invoice_id": None,
        "invoice_name": None,
        "line_items": []
    }

    state["invoice_data"] = {}

    try:
        if not ensure_sf_connected(sf_client):
            logger.error("❌ ")
            state["error"] = "Salesforce connection failed"
            return state

        order_items_soql = f"""
            SELECT Id,
                   Product2Id,
                   Product2.Name,
                   OrderItemNumber,
                    QuoteLineItem.LineNumber,
                   Quantity,
                   UnitPrice,
                   Rate__c,
                   Pricing_Model__c,
                   ServiceDate,
                   EndDate
            FROM OrderItem
            WHERE OrderId = '{order_id}'
        """
        order_items_results = sf_client.sf.query(order_items_soql)
        order_item_records = order_items_results.get("records", [])

        if not order_item_records:
            logger.warning(f"⚠️ No OrderItems found for Order '{order_id}'")
            state["error"] = f"No OrderItems found for Order {order_id}"
            return state

        product_to_orderitems = {}
        orderitem_details_map = {}

        for oi in order_item_records:
            product_id = oi.get("Product2Id")
            if not product_id:
                continue

            product_to_orderitems.setdefault(product_id, []).append(oi)

            orderitem_details_map[oi.get("Id")] = {
    "orderItemNumber": oi.get("OrderItemNumber"),
    "quoteLineItemId": oi.get("QuoteLineItemId"),
    "quoteLineNumber": (oi.get("QuoteLineItem") or {}).get("LineNumber"),
    "rate": _to_decimal(oi.get("Rate__c")) if oi.get("Rate__c") is not None else _to_decimal(oi.get("UnitPrice")),
    "pricingModel": oi.get("Pricing_Model__c") or "CPM",
    "productName": (oi.get("Product2") or {}).get("Name")
}

        ili_soql = f"""
            SELECT Id,
                   Name,
                   Invoice__c,
                   Invoice__r.Name,
                   Invoice__r.Invoice_Date__c,
                   Invoice__r.Status__c,
                   Invoice__r.Total_Amount__c,
                   Invoice__r.Total_Charges__c,
                   Invoice__r.Start_Date__c,
                   Invoice__r.End_Date__c,
                   Invoice__r.Billing_Account__c,
                   Invoice__r.Billing_Account__r.Name,
                   Product__c,
                   Pricing_Model__c,
                   Start_Date__c,
                   End_Date__c,
                   Billed_Impressions__c,
                   eCPM__c,
                   Price__c,
                   Line_Amount__c
            FROM Custom_Invoice_Line_Item__c
            WHERE Invoice__r.Order__c = '{order_id}'
        """
        ili_results = sf_client.sf.query(ili_soql)
        ili_records = ili_results.get("records", [])

        if not ili_records:
            logger.warning(f"⚠️ No Custom Invoice Line Items found for Order '{order_id}'")
            state["error"] = f"No Custom Invoice Line Items found for Order {order_id}"
            return state

        first_ili = ili_records[0]
        invoice_r = first_ili.get("Invoice__r") or {}
        invoice_id = first_ili.get("Invoice__c")

        delivery_info["invoice_id"] = invoice_id
        delivery_info["invoice_name"] = invoice_r.get("Name") or invoice_id

        advertiser_name = ((invoice_r.get("Billing_Account__r") or {}).get("Name")) or "Unknown"

        state["invoice_data"] = {
            "id": invoice_id,
            "name": invoice_r.get("Name") or invoice_id,
            "invoice_date": invoice_r.get("Invoice_Date__c"),
            "total_amount": _to_decimal(invoice_r.get("Total_Amount__c") or invoice_r.get("Total_Charges__c")),
            "status": invoice_r.get("Status__c"),
            "start": invoice_r.get("Start_Date__c") or first_ili.get("Start_Date__c"),
            "end": invoice_r.get("End_Date__c") or first_ili.get("End_Date__c"),
            "advertiser_id": invoice_r.get("Billing_Account__c"),
            "advertiser_name": advertiser_name,
        }

        logger.info(f"✅ Found Custom Invoice: ({invoice_id}) and {len(ili_records)} grouped line item(s)")

        for ili in ili_records:
            ili_id = ili.get("Id")
            product_id = ili.get("Product__c")

            if not product_id:
                logger.warning(f"⚠️ Custom Invoice Line Item {ili_id} has no Product__c. Skipping.")
                continue

            line_name = ili.get("Name") or ili_id
            billed_impressions = _to_decimal(ili.get("Billed_Impressions__c"))
            billed_amount = _to_decimal(ili.get("Price__c") or ili.get("Line_Amount__c"))
            pricing_model = ili.get("Pricing_Model__c") or "CPM"
            effective_rate = _to_decimal(ili.get("eCPM__c"))
            period_start = ili.get("Start_Date__c")
            period_end = ili.get("End_Date__c")

            matching_order_items = product_to_orderitems.get(product_id, [])
            if not matching_order_items:
                logger.warning(
                    f"⚠️ No matching OrderItems found for product {product_id} on invoice line {ili_id}"
                )
                continue

            product_name = None
            oi_numbers = []
            for oi in matching_order_items:
                if oi.get("Product2") and oi["Product2"].get("Name"):
                    product_name = oi["Product2"]["Name"]
                if oi.get("OrderItemNumber"):
                    oi_numbers.append(oi.get("OrderItemNumber"))

            product_name = product_name or line_name
            matching_oi_ids = [oi["Id"] for oi in matching_order_items if oi.get("Id")]
            in_clause = _safe_in_clause(matching_oi_ids)

            if not in_clause:
                logger.warning(f"⚠️ No valid OrderItem ids found for invoice line {ili_id}")
                continue

            if not period_start or not period_end:
                logger.warning(f"⚠️ Missing invoice line date range for grouped line {ili_id}. Skipping.")
                continue

            logger.info(
                f"   Fetching delivery blocks for product: {product_name} "
                f"(OrderItems: {', '.join(oi_numbers) if oi_numbers else 'N/A'}) "
                f"from {period_start} to {period_end}"
            )

            delivery_soql = f"""
                SELECT Id, Date__c, Gross__c, IVT__c, Viewability__c, Order_Product__c
                FROM Delivery_Data__c
                WHERE Order_Product__c IN ({in_clause})
                  AND Date__c >= {period_start}
                  AND Date__c <= {period_end}
                ORDER BY Date__c ASC
            """
            delivery_results = sf_client.sf.query_all(delivery_soql)
            delivery_records = delivery_results.get("records", [])

            item_data = {
                "ili_id": ili_id,
                "ili_name": product_name,
                "product_id": product_id,
                "oli_ids": matching_oi_ids,
                "oli_names": oi_numbers,
                "pricing_model": pricing_model,
                "effective_rate": effective_rate,
                "billed_impressions": billed_impressions,
                "billed_amount": billed_amount,
                "period_start": period_start,
                "period_end": period_end,
                "daily_blocks": []
            }

            for rec in delivery_records:
                order_product_id = rec.get("Order_Product__c")
                order_details = orderitem_details_map.get(order_product_id, {})

                item_data["daily_blocks"].append({
    "date": rec.get("Date__c"),
    "gross": _to_decimal(rec.get("Gross__c")),
    "ivt": _to_decimal(rec.get("IVT__c"), "0") / Decimal("100") if rec.get("IVT__c") is not None else Decimal("0"),
    "viewability": _to_decimal(rec.get("Viewability__c"), "0") / Decimal("100") if rec.get("Viewability__c") is not None else Decimal("0"),
    "orderProductId": order_product_id,
    "orderLineItemNumber": order_details.get("orderItemNumber") or "N/A",
    "quoteLineItemId": order_details.get("quoteLineItemId"),
    "quoteLineNumber": order_details.get("quoteLineNumber") or "N/A",
    "orderLineRate": _to_decimal(order_details.get("rate")),
    "orderLinePricingModel": order_details.get("pricingModel") or pricing_model
})

            delivery_info["line_items"].append(item_data)
            logger.info(f"      ✅ Added {len(delivery_records)} delivery block(s) for grouped product line {product_name}")

    except Exception as e:
        logger.error(f"❌ Salesforce query error: {e}")
        state["error"] = f"Salesforce query error: {str(e)}"

    state["delivery_data"] = delivery_info
    return state