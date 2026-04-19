# import json
# import logging
# from decimal import Decimal
# from typing import Any, Dict, List, Optional

# from agents.Reconciliation.state import ReconcillationState
# from core.helper import ensure_sf_connected, call_llm
# from agents.Reconciliation.nodes.utils import _to_decimal, _safe_in_clause, sf_client

# from mcp_module.Salesforcemcp.chromadbutils import (
#     ChromaDBManager,
#     chroma_client,
#     schema_data,
#     ensure_schema_initialized,
# )

# logger = logging.getLogger(__name__)

# chroma_manager = None


# def get_chroma_manager():
#     global chroma_manager
#     if chroma_manager is None:
#         chroma_manager = ChromaDBManager(chroma_client)
#     return chroma_manager


# def _load_schema_data():
#     """
#     Ensure schema is initialized and available.
#     """
#     global schema_data
#     ensure_schema_initialized()

#     from mcp_module.Salesforcemcp import chromadbutils
#     if hasattr(chromadbutils, "schema_data") and chromadbutils.schema_data:
#         schema_data = chromadbutils.schema_data

#     return schema_data


# def _clean_llm_json(raw_response: Any) -> Dict[str, Any]:
#     """
#     Normalize and parse LLM JSON response.
#     """
#     if isinstance(raw_response, str):
#         content = raw_response.strip()
#     elif isinstance(raw_response, list):
#         content = " ".join(str(x) for x in raw_response).strip()
#     else:
#         content = str(raw_response).strip()

#     if content.startswith("```json"):
#         content = content[7:]
#     elif content.startswith("```"):
#         content = content[3:]

#     if content.endswith("```"):
#         content = content[:-3]

#     content = content.strip()
#     return json.loads(content)


# def _normalize_relationship_path(field_path: Optional[str]) -> Optional[str]:
#     """
#     Fix common LLM mistake:
#     InvoiceId.Name -> Invoice.Name
#     InvoiceId.InvoiceDate -> Invoice.InvoiceDate
#     InvoiceId.AdvertiserId.Name -> Invoice.Advertiser.Name

#     Only for dotted relationship paths.
#     """
#     if not field_path or "." not in field_path:
#         return field_path

#     parts = field_path.split(".")
#     normalized = []

#     for i, part in enumerate(parts):
#         if i < len(parts) - 1 and part.endswith("Id"):
#             normalized.append(part[:-2])
#         else:
#             normalized.append(part)

#     return ".".join(normalized)


# def _normalize_invoice_schema_fields(plan: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Minimal correction for generated Invoice relationship fields so they stay aligned
#     with the semantic dictionary-backed schema names instead of invented names.
#     """
#     invoice_fields = plan.get("invoice_line_items", {}).get("fields", {})

#     for key in [
#         "invoice_name",
#         "invoice_date",
#         "invoice_status",
#         "invoice_total_amount",
#         "invoice_total_charges",
#         "invoice_start",
#         "invoice_end",
#         "advertiser_id",
#         "advertiser_name",
#         "order_lookup_path",
#     ]:
#         if key in invoice_fields:
#             invoice_fields[key] = _normalize_relationship_path(invoice_fields[key])

#     if invoice_fields.get("invoice_name") == "Invoice.Name":
#         invoice_fields["invoice_name"] = "Invoice.InvoiceNumber"

#     if invoice_fields.get("invoice_total_charges") == "Invoice.TotalCharges":
#         invoice_fields["invoice_total_charges"] = "Invoice.TotalChargeAmount"

#     if invoice_fields.get("advertiser_id") == "Invoice.AdvertiserId":
#         invoice_fields["advertiser_id"] = "Invoice.BillingAccountId"

#     if invoice_fields.get("advertiser_name") == "Invoice.Advertiser.Name":
#         invoice_fields["advertiser_name"] = "Invoice.BillingAccount.Name"

#     if invoice_fields.get("invoice_date") == "Invoice.InvoiceDate":
#         invoice_fields["invoice_date"] = "Invoice.PostedDate"

#     order_fields = plan.get("order_items", {}).get("fields", {})
#     if "quote_line_number" in order_fields:
#         order_fields["quote_line_number"] = _normalize_relationship_path(
#             order_fields["quote_line_number"]
#         )

#     return plan


# def _get_object_schema_block(object_name: str, user_query: str, top_k: int = 12) -> Dict[str, Any]:
#     """
#     Use Chroma to get relevant fields for a given object.
#     """
#     cm = get_chroma_manager()
#     field_results = cm.search_fields(object_name, user_query, top_k=top_k)

#     selected_fields = ["Id"]
#     field_metadata = []

#     for f in field_results:
#         field_name = f.get("field_name")
#         if field_name and field_name not in selected_fields:
#             selected_fields.append(field_name)
#             field_metadata.append({
#                 "field_name": field_name,
#                 "description": f.get("description", ""),
#                 "datatype": f.get("datatype", ""),
#                 "distance": f.get("distance")
#             })

#     try:
#         name_result = cm.search_fields(object_name, "Name", top_k=1)
#         if name_result and name_result[0].get("field_name") == "Name" and "Name" not in selected_fields:
#             selected_fields.append("Name")
#             field_metadata.append({
#                 "field_name": "Name",
#                 "description": name_result[0].get("description", ""),
#                 "datatype": name_result[0].get("datatype", ""),
#                 "distance": name_result[0].get("distance")
#             })
#     except Exception:
#         pass

#     return {
#         "object": object_name,
#         "fields": selected_fields,
#         "field_metadata": field_metadata
#     }


# async def generate_reconciliation_fetch_plan(
#     order_id: str,
#     user_goal: str = ""
# ) -> Dict[str, Any]:
#     """
#     Use ChromaDB + LLM to generate a schema-grounded fetch plan
#     for the three logical datasets needed by reconciliation:
#     1. order items
#     2. invoice line items
#     3. delivery data
#     """
#     _load_schema_data()
#     cm = get_chroma_manager()

#     order_query = (
#         "Salesforce object holding order product line items for a specific order filterby order_id. "
#         "Need fields for id, product lookup, product name, order item number, "
#         "quote line number, quantity, unit price, rate, pricing model, service start, service end."
#     )
#     invoice_line_query = (
#         "Salesforce InvoiceLine object linked to an Invoice and back to an Order. "
#         "Use only exact fields that exist in the provided schema candidates. "
#         "Need fields for line id, line name, invoice lookup, invoice line start date, invoice line end date, "
#         "invoice line status, product lookup, balance or charge amount, and relationship paths from InvoiceLine "
#         "to Invoice fields such as invoice number, posted date, total amount, total charge amount, and billing account. "
#         "Do not invent fields like Name, TotalCharges or AdvertiserId if they are not present in the schema field list."
#     )
#     delivery_query = (
#         "Salesforce object holding daily delivery metrics linked to an order product or order item. "
#         "Need fields for id, date, gross impressions, IVT percentage, viewability percentage, "
#         "order product lookup."
#     )

#     order_objects = cm.search_objects(order_query, top_k=5) or []
#     invoice_objects = cm.search_objects(invoice_line_query, top_k=5) or []
#     delivery_objects = cm.search_objects(delivery_query, top_k=5) or []

#     order_object = order_objects[0]["object_name"] if order_objects else "OrderItem"
#     invoice_object = invoice_objects[0]["object_name"] if invoice_objects else "Custom_Invoice_Line_Item__c"
#     delivery_object = delivery_objects[0]["object_name"] if delivery_objects else "Delivery_Data__c"

#     order_schema = _get_object_schema_block(order_object, order_query)
#     invoice_schema = _get_object_schema_block(invoice_object, invoice_line_query)
#     delivery_schema = _get_object_schema_block(delivery_object, delivery_query)

#     system_prompt = f"""
# You are a Salesforce schema planner for a reconciliation pipeline.

# Your job is to map three LOGICAL datasets into ACTUAL Salesforce object names and field API names using the provided schema candidates.
# You MUST return valid JSON only.

# The pipeline requires these three logical datasets:

# 1) order_items:
# - all line items belonging to the given Order
# - must provide:
#   id
#   product_id
#   product_name
#   order_item_number
#   quote_line_item_id (nullable if not available)
#   quote_line_number (nullable if not available)
#   quantity
#   unit_price
#   rate
#   pricing_model
#   service_start
#   service_end
#   order_lookup_field   <-- field used in WHERE clause with the order id

# 2) invoice_line_items:
# - all invoice lines belonging to the given Order, usually through parent invoice relationship
# - must provide:
#   id
#   name
#   invoice_id
#   invoice_name
#   invoice_date
#   invoice_status
#   invoice_total_amount
#   invoice_total_charges
#   invoice_start
#   invoice_end
#   advertiser_id
#   advertiser_name
#   product_id
#   pricing_model
#   start_date
#   end_date
#   billed_impressions
#   effective_rate
#   billed_amount_primary
#   billed_amount_fallback
#   order_lookup_path    <-- relationship path used in WHERE clause with the order id

# 3) delivery_data:
# - daily delivery rows belonging to one or more order line ids during a date range
# - must provide:
#   id
#   date
#   gross
#   ivt
#   viewability
#   order_product_id
#   order_product_lookup_field  <-- used in WHERE ... IN (...)
#   date_field                  <-- used in date range filter

# Rules:
# - Prefer exact API names from schema candidates.
# - Do not invent fields outside the supplied candidate objects unless absolutely standard and obvious.
# - Keep relationship fields in dotted SOQL path form when needed, e.g. Product2.Name or Invoice__r.Name.
# - For relationship traversal in SOQL, NEVER use an Id field in dotted paths.
#   Example:
#   - use Invoice.Name, not InvoiceId.Name
#   - use Invoice.InvoiceDate, not InvoiceId.InvoiceDate
#   - use Advertiser.Name, not AdvertiserId.Name
#   - for custom relationships use __r, not __c
# - Use ONLY field API names that appear in the provided candidate Fields list or Field metadata.
# - If a requested business concept is not present exactly, choose the nearest exact schema field from the candidate list. Do not invent names.
# - Do not assume the Invoice object has a Name field. Use the exact invoice identifier field returned in the candidate schema, such as InvoiceNumber, if Name is not present.
# - Return strictly this JSON shape:

# {{
#   "order_items": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "product_id": "...",
#       "product_name": "...",
#       "order_item_number": "...",
#       "quote_line_item_id": "...",
#       "quote_line_number": "...",
#       "quantity": "...",
#       "unit_price": "...",
#       "rate": "...",
#       "pricing_model": "...",
#       "service_start": "...",
#       "service_end": "...",
#       "order_lookup_field": "..."
#     }}
#   }},
#   "invoice_line_items": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "name": "...",
#       "invoice_id": "...",
#       "invoice_name": "...",
#       "invoice_date": "...",
#       "invoice_status": "...",
#       "invoice_total_amount": "...",
#       "invoice_total_charges": "...",
#       "invoice_start": "...",
#       "invoice_end": "...",
#       "advertiser_id": "...",
#       "advertiser_name": "...",
#       "product_id": "...",
#       "pricing_model": "...",
#       "start_date": "...",
#       "end_date": "...",
#       "billed_impressions": "...",
#       "effective_rate": "...",
#       "billed_amount_primary": "...",
#       "billed_amount_fallback": "...",
#       "order_lookup_path": "..."
#     }}
#   }},
#   "delivery_data": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "date": "...",
#       "gross": "...",
#       "ivt": "...",
#       "viewability": "...",
#       "order_product_id": "...",
#       "order_product_lookup_field": "...",
#       "date_field": "..."
#     }}
#   }}
# }}
# """.strip()

#     user_prompt = f"""
# Order id: {order_id}
# User goal: {user_goal or "Reconcile invoice vs delivery for this order"}

# Schema candidates:

# ORDER ITEM CANDIDATE
# Object: {order_schema["object"]}
# Fields: {", ".join(order_schema["fields"])}
# Field metadata:
# {json.dumps(order_schema["field_metadata"], indent=2)}

# INVOICE LINE ITEM CANDIDATE
# Object: {invoice_schema["object"]}
# Fields: {", ".join(invoice_schema["fields"])}
# Field metadata:
# {json.dumps(invoice_schema["field_metadata"], indent=2)}

# DELIVERY DATA CANDIDATE
# Object: {delivery_schema["object"]}
# Fields: {", ".join(delivery_schema["fields"])}
# Field metadata:
# {json.dumps(delivery_schema["field_metadata"], indent=2)}
# """.strip()

#     raw_response = await call_llm(
#         system_prompt=system_prompt,
#         user_prompt=user_prompt,
#         default_model="gpt-4o",
#         default_provider="openai",
#         default_temperature=0
#     )

#     plan = _clean_llm_json(raw_response)
#     plan = _normalize_invoice_schema_fields(plan)

#     logger.info(f"✅ Generated reconciliation fetch plan: {json.dumps(plan, indent=2)}")
#     return plan


# def _build_soql_select(object_name: str, field_map: Dict[str, str], exclude_keys: Optional[List[str]] = None) -> str:
#     """
#     Build SELECT clause from field aliases -> actual api field/path.
#     """
#     exclude_keys = exclude_keys or []
#     seen = set()
#     fields = []

#     for logical_name, actual_field in field_map.items():
#         if logical_name in exclude_keys:
#             continue
#         if actual_field and actual_field not in seen:
#             seen.add(actual_field)
#             fields.append(actual_field)

#     if not fields:
#         raise ValueError(f"No fields available to build SELECT for {object_name}")

#     return ", ".join(fields)


# def _get_value(record: Dict[str, Any], field_path: str) -> Any:
#     """
#     Read nested SOQL relationship paths like Invoice__r.Name from a Salesforce record.
#     """
#     if not field_path:
#         return None

#     parts = field_path.split(".")
#     current = record
#     for p in parts:
#         if current is None:
#             return None
#         if not isinstance(current, dict):
#             return None
#         current = current.get(p)
#     return current


# async def fetch_delivery_data_node(state: ReconcillationState) -> ReconcillationState:
#     """
#     Fetch grouped invoice lines, matching order items, and delivery data.
#     Object names and fields are resolved dynamically using ChromaDB + LLM,
#     but the returned state structure stays exactly the same as before.
#     """
#     order_id = state.get("record_id")
#     user_goal = state.get("user_goal", "")

#     logger.info(f"PS1504🔌 [ENTERING] fetch_delivery_data_node | Order ID: {order_id}")

#     if not order_id:
#         logger.error("PS1504🔌 ❌ No record_id found in state. Cannot fetch data.")
#         state["error"] = "No Order ID provided context."
#         return state

#     delivery_info = {
#         "order_id": order_id,
#         "invoice_id": None,
#         "invoice_name": None,
#         "line_items": []
#     }
#     state["invoice_data"] = {}

#     try:
#         logger.info("PS1504🔌 🔌 Checking Salesforce connection...")
#         if not ensure_sf_connected(sf_client):
#             logger.error("PS1504🔌 ❌ Salesforce connection failed.")
#             state["error"] = "Salesforce connection failed"
#             return state
#         logger.info("PS1504🔌 ✅ Salesforce connected.")

#         plan = await generate_reconciliation_fetch_plan(order_id=order_id, user_goal=user_goal)

#         order_cfg = plan["order_items"]
#         invoice_cfg = plan["invoice_line_items"]
#         delivery_cfg = plan["delivery_data"]

#         order_fields = order_cfg["fields"]
#         invoice_fields = invoice_cfg["fields"]
#         delivery_fields = delivery_cfg["fields"]

#         order_select = _build_soql_select(
#             order_cfg["object"],
#             order_fields,
#             exclude_keys=["order_lookup_field"]
#         )

#         order_soql = f"""
#             SELECT {order_select}
#             FROM {order_cfg["object"]}
#             WHERE {order_fields["order_lookup_field"]} = '{order_id}'
#         """
#         logger.info(f"PS1504🔌 🔍 [SOQL] Dynamic Order Items Query:\n{order_soql}")
#         order_results = sf_client.sf.query(order_soql)
#         order_item_records = order_results.get("records", [])

#         if not order_item_records:
#             logger.warning(f"PS1504🔌 ⚠️ No OrderItems found for Order '{order_id}'")
#             state["error"] = f"No OrderItems found for Order {order_id}"
#             return state

#         logger.info(f"PS1504🔌 📦 Found {len(order_item_records)} order item records.")

#         product_to_orderitems: Dict[str, List[Dict[str, Any]]] = {}
#         orderitem_details_map: Dict[str, Dict[str, Any]] = {}

#         for oi in order_item_records:
#             oi_id = _get_value(oi, order_fields["id"])
#             product_id = _get_value(oi, order_fields["product_id"])

#             if not product_id or not oi_id:
#                 continue

#             product_to_orderitems.setdefault(product_id, []).append(oi)

#             orderitem_details_map[oi_id] = {
#                 "orderItemNumber": _get_value(oi, order_fields["order_item_number"]),
#                 "quoteLineItemId": _get_value(oi, order_fields["quote_line_item_id"]),
#                 "quoteLineNumber": _get_value(oi, order_fields["quote_line_number"]),
#                 "rate": _to_decimal(_get_value(oi, order_fields["rate"]))
#                 if _get_value(oi, order_fields["rate"]) is not None
#                 else _to_decimal(_get_value(oi, order_fields["unit_price"])),
#                 "pricingModel": _get_value(oi, order_fields["pricing_model"]) or "CPM",
#                 "productName": _get_value(oi, order_fields["product_name"])
#             }

#         invoice_select = _build_soql_select(
#             invoice_cfg["object"],
#             invoice_fields,
#             exclude_keys=["order_lookup_path"]
#         )

#         invoice_soql = f"""
#             SELECT {invoice_select}
#             FROM {invoice_cfg["object"]}
#             WHERE {invoice_fields["order_lookup_path"]} = '{order_id}'
#         """
#         logger.info(f"PS1504🔌 🔍 [SOQL] Dynamic Invoice Line Query:\n{invoice_soql}")
#         ili_results = sf_client.sf.query(invoice_soql)
#         ili_records = ili_results.get("records", [])

#         if not ili_records:
#             logger.warning(f"PS1504🔌 ⚠️ No Invoice Line Items found for Order '{order_id}'")
#             state["error"] = f"No Invoice Line Items found for Order {order_id}"
#             return state

#         first_ili = ili_records[0]
#         invoice_id = _get_value(first_ili, invoice_fields["invoice_id"])
#         invoice_name = _get_value(first_ili, invoice_fields["invoice_name"]) or invoice_id

#         delivery_info["invoice_id"] = invoice_id
#         delivery_info["invoice_name"] = invoice_name

#         state["invoice_data"] = {
#             "id": invoice_id,
#             "name": invoice_name,
#             "invoice_date": _get_value(first_ili, invoice_fields["invoice_date"]),
#             "total_amount": _to_decimal(
#                 _get_value(first_ili, invoice_fields["invoice_total_amount"])
#                 or _get_value(first_ili, invoice_fields["invoice_total_charges"])
#             ),
#             "status": _get_value(first_ili, invoice_fields["invoice_status"]),
#             "start": _get_value(first_ili, invoice_fields["invoice_start"]) or _get_value(first_ili, invoice_fields["start_date"]),
#             "end": _get_value(first_ili, invoice_fields["invoice_end"]) or _get_value(first_ili, invoice_fields["end_date"]),
#             "advertiser_id": _get_value(first_ili, invoice_fields["advertiser_id"]),
#             "advertiser_name": _get_value(first_ili, invoice_fields["advertiser_name"]) or "Unknown",
#         }

#         logger.info(f"PS1504🔌 🧾 Linked to Invoice: {state['invoice_data']['name']} ({invoice_id})")
#         logger.info(f"PS1504🔌 🔄 Processing {len(ili_records)} invoice line items...")

#         for ili in ili_records:
#             ili_id = _get_value(ili, invoice_fields["id"])
#             product_id = _get_value(ili, invoice_fields["product_id"])

#             if not product_id:
#                 logger.warning(f"PS1504🔌 ⚠️ Invoice line {ili_id} missing product field. Skipping.")
#                 continue

#             line_name = _get_value(ili, invoice_fields["name"]) or ili_id
#             pricing_model = _get_value(ili, invoice_fields["pricing_model"]) or "CPM"
#             period_start = _get_value(ili, invoice_fields["start_date"])
#             period_end = _get_value(ili, invoice_fields["end_date"])

#             matching_order_items = product_to_orderitems.get(product_id, [])
#             if not matching_order_items:
#                 logger.warning(f"PS1504🔌 ⚠️ No matching OrderItems for Product {product_id} (ILI: {ili_id})")
#                 continue

#             matching_oi_ids = [
#                 _get_value(oi, order_fields["id"])
#                 for oi in matching_order_items
#                 if _get_value(oi, order_fields["id"])
#             ]
#             in_clause = _safe_in_clause(matching_oi_ids)

#             if not in_clause or not period_start or not period_end:
#                 logger.warning(f"PS1504🔌 ⚠️ Missing critical data (Dates/IDs) for ILI {ili_id}. Skipping.")
#                 continue

#             delivery_select = _build_soql_select(
#                 delivery_cfg["object"],
#                 delivery_fields,
#                 exclude_keys=["order_product_lookup_field", "date_field"]
#             )

#             delivery_soql = f"""
#                 SELECT {delivery_select}
#                 FROM {delivery_cfg["object"]}
#                 WHERE {delivery_fields["order_product_lookup_field"]} IN ({in_clause})
#                   AND {delivery_fields["date_field"]} >= {period_start}
#                   AND {delivery_fields["date_field"]} <= {period_end}
#                 ORDER BY {delivery_fields["date_field"]} ASC
#             """
#             logger.info(f"PS1504🔌    📊 Dynamic Delivery Query for {line_name}:\n{delivery_soql}")

#             delivery_results = sf_client.sf.query_all(delivery_soql)
#             delivery_records = delivery_results.get("records", [])

#             item_data = {
#                 "ili_id": ili_id,
#                 "ili_name": line_name,
#                 "product_id": product_id,
#                 "oli_ids": matching_oi_ids,
#                 "oli_names": [
#                     _get_value(oi, order_fields["order_item_number"])
#                     for oi in matching_order_items
#                 ],
#                 "pricing_model": pricing_model,
#                 "effective_rate": _to_decimal(_get_value(ili, invoice_fields["effective_rate"])),
#                 "billed_impressions": _to_decimal(_get_value(ili, invoice_fields["billed_impressions"])),
#                 "billed_amount": _to_decimal(
#                     _get_value(ili, invoice_fields["billed_amount_primary"])
#                     or _get_value(ili, invoice_fields["billed_amount_fallback"])
#                 ),
#                 "period_start": period_start,
#                 "period_end": period_end,
#                 "daily_blocks": []
#             }

#             for rec in delivery_records:
#                 order_product_id = _get_value(rec, delivery_fields["order_product_id"])
#                 order_details = orderitem_details_map.get(order_product_id, {})

#                 item_data["daily_blocks"].append({
#                     "date": _get_value(rec, delivery_fields["date"]),
#                     "gross": _to_decimal(_get_value(rec, delivery_fields["gross"])),
#                     "ivt": _to_decimal(_get_value(rec, delivery_fields["ivt"]), "0") / Decimal("100")
#                     if _get_value(rec, delivery_fields["ivt"]) is not None else Decimal("0"),
#                     "viewability": _to_decimal(_get_value(rec, delivery_fields["viewability"]), "0") / Decimal("100")
#                     if _get_value(rec, delivery_fields["viewability"]) is not None else Decimal("0"),
#                     "orderProductId": order_product_id,
#                     "orderLineItemNumber": order_details.get("orderItemNumber") or "N/A",
#                     "quoteLineItemId": order_details.get("quoteLineItemId"),
#                     "quoteLineNumber": order_details.get("quoteLineNumber") or "N/A",
#                     "orderLineRate": _to_decimal(order_details.get("rate")),
#                     "orderLinePricingModel": order_details.get("pricingModel") or pricing_model
#                 })

#             delivery_info["line_items"].append(item_data)
#             logger.info(f"PS1504🔌       ✅ Attached {len(delivery_records)} delivery blocks for {line_name}")

#     except Exception as e:
#         logger.error(f"PS1504🔌 💥 CRITICAL Salesforce error in fetch_delivery_data_node: {e}", exc_info=True)
#         state["error"] = f"Salesforce query error: {str(e)}"

#     state["delivery_data"] = delivery_info
#     processed_count = len(delivery_info["line_items"])
#     logger.info(f"PS1504🔌 📤 [EXITING] fetch_delivery_data_node | Processed {processed_count} grouped line items.")

#     return state





# no hardcoding but where clause orderitemid

# import json
# import logging
# from decimal import Decimal
# from typing import Any, Dict, List, Optional

# from agents.Reconciliation.state import ReconcillationState
# from core.helper import ensure_sf_connected, call_llm
# from agents.Reconciliation.nodes.utils import _to_decimal, _safe_in_clause, sf_client

# # Chroma imports - same pattern as your reference code
# from mcp_module.Salesforcemcp.chromadbutils import ChromaDBManager, chroma_client, schema_data, ensure_schema_initialized

# logger = logging.getLogger(__name__)

# chroma_manager = None


# def get_chroma_manager():
#     global chroma_manager
#     if chroma_manager is None:
#         chroma_manager = ChromaDBManager(chroma_client)
#     return chroma_manager


# def _load_schema_data():
#     """
#     Ensure schema is initialized and available.
#     """
#     global schema_data
#     ensure_schema_initialized()

#     from mcp_module.Salesforcemcp import chromadbutils
#     if hasattr(chromadbutils, "schema_data") and chromadbutils.schema_data:
#         schema_data = chromadbutils.schema_data

#     return schema_data


# def _clean_llm_json(raw_response: Any) -> Dict[str, Any]:
#     """
#     Normalize and parse LLM JSON response.
#     """
#     if isinstance(raw_response, str):
#         content = raw_response.strip()
#     elif isinstance(raw_response, list):
#         content = " ".join(str(x) for x in raw_response).strip()
#     else:
#         content = str(raw_response).strip()

#     if content.startswith("```json"):
#         content = content[7:]
#     elif content.startswith("```"):
#         content = content[3:]

#     if content.endswith("```"):
#         content = content[:-3]

#     content = content.strip()
#     return json.loads(content)


# def _get_object_schema_block(object_name: str, user_query: str, top_k: int = 12) -> Dict[str, Any]:
#     """
#     Use Chroma to get relevant fields for a given object.
#     """
#     cm = get_chroma_manager()
#     field_results = cm.search_fields(object_name, user_query, top_k=top_k)

#     selected_fields = ["Id"]
#     field_metadata = []

#     for f in field_results:
#         field_name = f.get("field_name")
#         if field_name and field_name not in selected_fields:
#             selected_fields.append(field_name)
#             field_metadata.append({
#                 "field_name": field_name,
#                 "description": f.get("description", ""),
#                 "datatype": f.get("datatype", ""),
#                 "distance": f.get("distance")
#             })

#     # try to include Name if it exists semantically
#     try:
#         name_result = cm.search_fields(object_name, "Name", top_k=1)
#         if name_result and name_result[0].get("field_name") == "Name" and "Name" not in selected_fields:
#             selected_fields.append("Name")
#             field_metadata.append({
#                 "field_name": "Name",
#                 "description": name_result[0].get("description", ""),
#                 "datatype": name_result[0].get("datatype", ""),
#                 "distance": name_result[0].get("distance")
#             })
#     except Exception:
#         pass

#     return {
#         "object": object_name,
#         "fields": selected_fields,
#         "field_metadata": field_metadata
#     }


# async def generate_reconciliation_fetch_plan(
#     order_id: str,
#     user_goal: str = ""
# ) -> Dict[str, Any]:
#     """
#     Use ChromaDB + LLM to generate a schema-grounded fetch plan
#     for the three logical datasets needed by reconciliation:
#     1. order items
#     2. invoice line items
#     3. delivery data
#     """
#     _load_schema_data()
#     cm = get_chroma_manager()

#     # Semantic object search for the three logical groups
#     order_query = (
#         "Salesforce object holding order product line items for a specific order filterby order_id. "
#         "Need fields for id, product lookup, product name, order item number, "
#         "quote line number, quantity, unit price, rate, pricing model, service start, service end."
#     )
#     invoice_line_query = (
#         "Salesforce object holding invoice line items linked to an order through invoice. "
#         "Need fields for line id, line name, invoice id, invoice name, invoice date, invoice status, "
#         "invoice total amount, invoice total charges, invoice start date, invoice end date, "
#         "billing account id, billing account name, product lookup, pricing model, start date, end date, "
#         "billed impressions, ecpm, price, line amount."
#     )
#     delivery_query = (
#         "Salesforce object holding daily delivery metrics linked to an order product or order item. "
#         "Need fields for id, date, gross impressions, IVT percentage, viewability percentage, "
#         "order product lookup."
#     )

#     order_objects = cm.search_objects(order_query, top_k=5) or []
#     invoice_objects = cm.search_objects(invoice_line_query, top_k=5) or []
#     delivery_objects = cm.search_objects(delivery_query, top_k=5) or []

#     # Pick top candidate object names
#     order_object = order_objects[0]["object_name"] if order_objects else "OrderItem"
#     invoice_object = invoice_objects[0]["object_name"] if invoice_objects else "Custom_Invoice_Line_Item__c"
#     delivery_object = delivery_objects[0]["object_name"] if delivery_objects else "Delivery_Data__c"

#     order_schema = _get_object_schema_block(order_object, order_query)
#     invoice_schema = _get_object_schema_block(invoice_object, invoice_line_query)
#     delivery_schema = _get_object_schema_block(delivery_object, delivery_query)

#     system_prompt = f"""
# You are a Salesforce schema planner for a reconciliation pipeline.

# Your job is to map three LOGICAL datasets into ACTUAL Salesforce object names and field API names using the provided schema candidates.
# You MUST return valid JSON only.

# The pipeline requires these three logical datasets:

# 1) order_items:
# - all line items belonging to the given Order
# - must provide:
#   id
#   product_id
#   product_name
#   order_item_number
#   quote_line_item_id (nullable if not available)
#   quote_line_number (nullable if not available)
#   quantity
#   unit_price
#   rate
#   pricing_model
#   service_start
#   service_end
#   order_lookup_field   <-- field used in WHERE clause with the order id

# 2) invoice_line_items:
# - all invoice lines belonging to the given Order, usually through parent invoice relationship
# - must provide:
#   id
#   name
#   invoice_id
#   invoice_name
#   invoice_date
#   invoice_status
#   invoice_total_amount
#   invoice_total_charges
#   invoice_start
#   invoice_end
#   advertiser_id
#   advertiser_name
#   product_id
#   pricing_model
#   start_date
#   end_date
#   billed_impressions
#   effective_rate
#   billed_amount_primary
#   billed_amount_fallback
#   order_lookup_path    <-- relationship path used in WHERE clause with the order id

# 3) delivery_data:
# - daily delivery rows belonging to one or more order line ids during a date range
# - must provide:
#   id
#   date
#   gross
#   ivt
#   viewability
#   order_product_id
#   order_product_lookup_field  <-- used in WHERE ... IN (...)
#   date_field                  <-- used in date range filter

# Rules:
# - Prefer exact API names from schema candidates.
# - Do not invent fields outside the supplied candidate objects unless absolutely standard and obvious.
# - Keep relationship fields in dotted SOQL path form when needed, e.g. Product2.Name or Invoice__r.Name.
# - Return strictly this JSON shape:

# {{
#   "order_items": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "product_id": "...",
#       "product_name": "...",
#       "order_item_number": "...",
#       "quote_line_item_id": "...",
#       "quote_line_number": "...",
#       "quantity": "...",
#       "unit_price": "...",
#       "rate": "...",
#       "pricing_model": "...",
#       "service_start": "...",
#       "service_end": "...",
#       "order_lookup_field": "..."
#     }}
#   }},
#   "invoice_line_items": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "name": "...",
#       "invoice_id": "...",
#       "invoice_name": "...",
#       "invoice_date": "...",
#       "invoice_status": "...",
#       "invoice_total_amount": "...",
#       "invoice_total_charges": "...",
#       "invoice_start": "...",
#       "invoice_end": "...",
#       "advertiser_id": "...",
#       "advertiser_name": "...",
#       "product_id": "...",
#       "pricing_model": "...",
#       "start_date": "...",
#       "end_date": "...",
#       "billed_impressions": "...",
#       "effective_rate": "...",
#       "billed_amount_primary": "...",
#       "billed_amount_fallback": "...",
#       "order_lookup_path": "..."
#     }}
#   }},
#   "delivery_data": {{
#     "object": "...",
#     "fields": {{
#       "id": "...",
#       "date": "...",
#       "gross": "...",
#       "ivt": "...",
#       "viewability": "...",
#       "order_product_id": "...",
#       "order_product_lookup_field": "...",
#       "date_field": "..."
#     }}
#   }}
# }}
# """.strip()

#     user_prompt = f"""
# Order id: {order_id}
# User goal: {user_goal or "Reconcile invoice vs delivery for this order"}

# Schema candidates:

# ORDER ITEM CANDIDATE
# Object: {order_schema["object"]}
# Fields: {", ".join(order_schema["fields"])}
# Field metadata:
# {json.dumps(order_schema["field_metadata"], indent=2)}

# INVOICE LINE ITEM CANDIDATE
# Object: {invoice_schema["object"]}
# Fields: {", ".join(invoice_schema["fields"])}
# Field metadata:
# {json.dumps(invoice_schema["field_metadata"], indent=2)}

# DELIVERY DATA CANDIDATE
# Object: {delivery_schema["object"]}
# Fields: {", ".join(delivery_schema["fields"])}
# Field metadata:
# {json.dumps(delivery_schema["field_metadata"], indent=2)}
# """.strip()

#     raw_response = await call_llm(
#         system_prompt=system_prompt,
#         user_prompt=user_prompt,
#         default_model="gpt-4o",
#         default_provider="openai",
#         default_temperature=0
#     )

#     plan = _clean_llm_json(raw_response)
#     logger.info(f"✅ Generated reconciliation fetch plan: {json.dumps(plan, indent=2)}")
#     return plan


# def _build_soql_select(object_name: str, field_map: Dict[str, str], exclude_keys: Optional[List[str]] = None) -> str:
#     """
#     Build SELECT clause from field aliases -> actual api field/path.
#     """
#     exclude_keys = exclude_keys or []
#     seen = set()
#     fields = []

#     for logical_name, actual_field in field_map.items():
#         if logical_name in exclude_keys:
#             continue
#         if actual_field and actual_field not in seen:
#             seen.add(actual_field)
#             fields.append(actual_field)

#     if not fields:
#         raise ValueError(f"No fields available to build SELECT for {object_name}")

#     return ", ".join(fields)


# def _get_value(record: Dict[str, Any], field_path: str) -> Any:
#     """
#     Read nested SOQL relationship paths like Invoice__r.Name from a Salesforce record.
#     """
#     if not field_path:
#         return None

#     parts = field_path.split(".")
#     current = record
#     for p in parts:
#         if current is None:
#             return None
#         if not isinstance(current, dict):
#             return None
#         current = current.get(p)
#     return current


# async def fetch_delivery_data_node(state: ReconcillationState) -> ReconcillationState:
#     """
#     Fetch grouped invoice lines, matching order items, and delivery data.
#     Object names and fields are resolved dynamically using ChromaDB + LLM,
#     but the returned state structure stays exactly the same as before.
#     """
#     order_id = state.get("record_id")
#     user_goal = state.get("user_goal", "")

#     logger.info(f"PS1504🔌 [ENTERING] fetch_delivery_data_node | Order ID: {order_id}")

#     if not order_id:
#         logger.error("PS1504🔌 ❌ No record_id found in state. Cannot fetch data.")
#         state["error"] = "No Order ID provided context."
#         return state

#     delivery_info = {
#         "order_id": order_id,
#         "invoice_id": None,
#         "invoice_name": None,
#         "line_items": []
#     }
#     state["invoice_data"] = {}

#     try:
#         logger.info("PS1504🔌 🔌 Checking Salesforce connection...")
#         if not ensure_sf_connected(sf_client):
#             logger.error("PS1504🔌 ❌ Salesforce connection failed.")
#             state["error"] = "Salesforce connection failed"
#             return state
#         logger.info("PS1504🔌 ✅ Salesforce connected.")

#         # ---------------------------
#         # 1) Build fetch plan dynamically
#         # ---------------------------
#         plan = await generate_reconciliation_fetch_plan(order_id=order_id, user_goal=user_goal)

#         order_cfg = plan["order_items"]
#         invoice_cfg = plan["invoice_line_items"]
#         delivery_cfg = plan["delivery_data"]

#         order_fields = order_cfg["fields"]
#         invoice_fields = invoice_cfg["fields"]
#         delivery_fields = delivery_cfg["fields"]

#         # ---------------------------
#         # 2) Fetch Order Items dynamically
#         # ---------------------------
#         order_select = _build_soql_select(
#             order_cfg["object"],
#             order_fields,
#             exclude_keys=["order_lookup_field"]
#         )

#         order_soql = f"""
#             SELECT {order_select}
#             FROM {order_cfg["object"]}
#             WHERE {order_fields["order_lookup_field"]} = '{order_id}'
#         """
#         logger.info(f"PS1504🔌 🔍 [SOQL] Dynamic Order Items Query:\n{order_soql}")
#         order_results = sf_client.sf.query(order_soql)
#         order_item_records = order_results.get("records", [])

#         if not order_item_records:
#             logger.warning(f"PS1504🔌 ⚠️ No OrderItems found for Order '{order_id}'")
#             state["error"] = f"No OrderItems found for Order {order_id}"
#             return state

#         logger.info(f"PS1504🔌 📦 Found {len(order_item_records)} order item records.")

#         product_to_orderitems: Dict[str, List[Dict[str, Any]]] = {}
#         orderitem_details_map: Dict[str, Dict[str, Any]] = {}

#         for oi in order_item_records:
#             oi_id = _get_value(oi, order_fields["id"])
#             product_id = _get_value(oi, order_fields["product_id"])

#             if not product_id or not oi_id:
#                 continue

#             product_to_orderitems.setdefault(product_id, []).append(oi)

#             orderitem_details_map[oi_id] = {
#                 "orderItemNumber": _get_value(oi, order_fields["order_item_number"]),
#                 "quoteLineItemId": _get_value(oi, order_fields["quote_line_item_id"]),
#                 "quoteLineNumber": _get_value(oi, order_fields["quote_line_number"]),
#                 "rate": _to_decimal(_get_value(oi, order_fields["rate"]))
#                 if _get_value(oi, order_fields["rate"]) is not None
#                 else _to_decimal(_get_value(oi, order_fields["unit_price"])),
#                 "pricingModel": _get_value(oi, order_fields["pricing_model"]) or "CPM",
#                 "productName": _get_value(oi, order_fields["product_name"])
#             }

#         # ---------------------------
#         # 3) Fetch Invoice Line Items dynamically
#         # ---------------------------
#         invoice_select = _build_soql_select(
#             invoice_cfg["object"],
#             invoice_fields,
#             exclude_keys=["order_lookup_path"]
#         )

#         invoice_soql = f"""
#             SELECT {invoice_select}
#             FROM {invoice_cfg["object"]}
#             WHERE {invoice_fields["order_lookup_path"]} = '{order_id}'
#         """
#         logger.info(f"PS1504🔌 🔍 [SOQL] Dynamic Invoice Line Query:\n{invoice_soql}")
#         ili_results = sf_client.sf.query(invoice_soql)
#         ili_records = ili_results.get("records", [])

#         if not ili_records:
#             logger.warning(f"PS1504🔌 ⚠️ No Invoice Line Items found for Order '{order_id}'")
#             state["error"] = f"No Invoice Line Items found for Order {order_id}"
#             return state

#         first_ili = ili_records[0]
#         invoice_id = _get_value(first_ili, invoice_fields["invoice_id"])
#         invoice_name = _get_value(first_ili, invoice_fields["invoice_name"]) or invoice_id

#         delivery_info["invoice_id"] = invoice_id
#         delivery_info["invoice_name"] = invoice_name

#         state["invoice_data"] = {
#             "id": invoice_id,
#             "name": invoice_name,
#             "invoice_date": _get_value(first_ili, invoice_fields["invoice_date"]),
#             "total_amount": _to_decimal(
#                 _get_value(first_ili, invoice_fields["invoice_total_amount"])
#                 or _get_value(first_ili, invoice_fields["invoice_total_charges"])
#             ),
#             "status": _get_value(first_ili, invoice_fields["invoice_status"]),
#             "start": _get_value(first_ili, invoice_fields["invoice_start"]) or _get_value(first_ili, invoice_fields["start_date"]),
#             "end": _get_value(first_ili, invoice_fields["invoice_end"]) or _get_value(first_ili, invoice_fields["end_date"]),
#             "advertiser_id": _get_value(first_ili, invoice_fields["advertiser_id"]),
#             "advertiser_name": _get_value(first_ili, invoice_fields["advertiser_name"]) or "Unknown",
#         }

#         logger.info(f"PS1504🔌 🧾 Linked to Invoice: {state['invoice_data']['name']} ({invoice_id})")

#         # ---------------------------
#         # 4) For each invoice line, fetch delivery dynamically
#         # ---------------------------
#         logger.info(f"PS1504🔌 🔄 Processing {len(ili_records)} invoice line items...")

#         for ili in ili_records:
#             ili_id = _get_value(ili, invoice_fields["id"])
#             product_id = _get_value(ili, invoice_fields["product_id"])

#             if not product_id:
#                 logger.warning(f"PS1504🔌 ⚠️ Invoice line {ili_id} missing product field. Skipping.")
#                 continue

#             line_name = _get_value(ili, invoice_fields["name"]) or ili_id
#             pricing_model = _get_value(ili, invoice_fields["pricing_model"]) or "CPM"
#             period_start = _get_value(ili, invoice_fields["start_date"])
#             period_end = _get_value(ili, invoice_fields["end_date"])

#             matching_order_items = product_to_orderitems.get(product_id, [])
#             if not matching_order_items:
#                 logger.warning(f"PS1504🔌 ⚠️ No matching OrderItems for Product {product_id} (ILI: {ili_id})")
#                 continue

#             matching_oi_ids = [
#                 _get_value(oi, order_fields["id"])
#                 for oi in matching_order_items
#                 if _get_value(oi, order_fields["id"])
#             ]
#             in_clause = _safe_in_clause(matching_oi_ids)

#             if not in_clause or not period_start or not period_end:
#                 logger.warning(f"PS1504🔌 ⚠️ Missing critical data (Dates/IDs) for ILI {ili_id}. Skipping.")
#                 continue

#             delivery_select = _build_soql_select(
#                 delivery_cfg["object"],
#                 delivery_fields,
#                 exclude_keys=["order_product_lookup_field", "date_field"]
#             )

#             delivery_soql = f"""
#                 SELECT {delivery_select}
#                 FROM {delivery_cfg["object"]}
#                 WHERE {delivery_fields["order_product_lookup_field"]} IN ({in_clause})
#                   AND {delivery_fields["date_field"]} >= {period_start}
#                   AND {delivery_fields["date_field"]} <= {period_end}
#                 ORDER BY {delivery_fields["date_field"]} ASC
#             """
#             logger.info(f"PS1504🔌    📊 Dynamic Delivery Query for {line_name}:\n{delivery_soql}")

#             delivery_results = sf_client.sf.query_all(delivery_soql)
#             delivery_records = delivery_results.get("records", [])

#             item_data = {
#                 "ili_id": ili_id,
#                 "ili_name": line_name,
#                 "product_id": product_id,
#                 "oli_ids": matching_oi_ids,
#                 "oli_names": [
#                     _get_value(oi, order_fields["order_item_number"])
#                     for oi in matching_order_items
#                 ],
#                 "pricing_model": pricing_model,
#                 "effective_rate": _to_decimal(_get_value(ili, invoice_fields["effective_rate"])),
#                 "billed_impressions": _to_decimal(_get_value(ili, invoice_fields["billed_impressions"])),
#                 "billed_amount": _to_decimal(
#                     _get_value(ili, invoice_fields["billed_amount_primary"])
#                     or _get_value(ili, invoice_fields["billed_amount_fallback"])
#                 ),
#                 "period_start": period_start,
#                 "period_end": period_end,
#                 "daily_blocks": []
#             }

#             for rec in delivery_records:
#                 order_product_id = _get_value(rec, delivery_fields["order_product_id"])
#                 order_details = orderitem_details_map.get(order_product_id, {})

#                 item_data["daily_blocks"].append({
#                     "date": _get_value(rec, delivery_fields["date"]),
#                     "gross": _to_decimal(_get_value(rec, delivery_fields["gross"])),
#                     "ivt": _to_decimal(_get_value(rec, delivery_fields["ivt"]), "0") / Decimal("100")
#                     if _get_value(rec, delivery_fields["ivt"]) is not None else Decimal("0"),
#                     "viewability": _to_decimal(_get_value(rec, delivery_fields["viewability"]), "0") / Decimal("100")
#                     if _get_value(rec, delivery_fields["viewability"]) is not None else Decimal("0"),
#                     "orderProductId": order_product_id,
#                     "orderLineItemNumber": order_details.get("orderItemNumber") or "N/A",
#                     "quoteLineItemId": order_details.get("quoteLineItemId"),
#                     "quoteLineNumber": order_details.get("quoteLineNumber") or "N/A",
#                     "orderLineRate": _to_decimal(order_details.get("rate")),
#                     "orderLinePricingModel": order_details.get("pricingModel") or pricing_model
#                 })

#             delivery_info["line_items"].append(item_data)
#             logger.info(f"PS1504🔌       ✅ Attached {len(delivery_records)} delivery blocks for {line_name}")

#     except Exception as e:
#         logger.error(f"PS1504🔌 💥 CRITICAL Salesforce error in fetch_delivery_data_node: {e}", exc_info=True)
#         state["error"] = f"Salesforce query error: {str(e)}"

#     state["delivery_data"] = delivery_info
#     processed_count = len(delivery_info["line_items"])
#     logger.info(f"PS1504🔌 📤 [EXITING] fetch_delivery_data_node | Processed {processed_count} grouped line items.")

#     return state




# OG - 1

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