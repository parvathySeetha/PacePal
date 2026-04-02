# # nodes/completion.py

# from langchain_openai import ChatOpenAI
# from langchain_core.messages import HumanMessage, AIMessage
# import logging
# import json
# import os
# from core.state import MarketingState


# def get_available_fields(obj_type):
#     """
#     Reads schema_metadata.json to return available fields for the object.
#     Returns: [{label: 'Status', name: 'Status', type: 'picklist', picklistValues: [...]}, ...]
#     """
#     try:
#         base_dir = os.path.dirname(os.path.abspath(__file__))
#         project_root = os.path.dirname(base_dir)
#         schema_path = os.path.join(project_root, "schema_metadata.json")
        
#         if not os.path.exists(schema_path):
#             logging.warning(f"Schema file not found: {schema_path}")
#             return []
        
#         with open(schema_path, "r", encoding="utf-8") as f:
#             data = json.load(f)
        
#         # Find object (case-insensitive)
#         obj_meta = {}
#         if isinstance(data, list):
#             obj_meta = next((item for item in data if item["object"].lower() == str(obj_type).lower()), {})
#         else:
#             obj_meta = data.get(obj_type, {})
        
#         fields = obj_meta.get("fields", [])
        
#         # Transform for UI
#         ui_fields = []
#         for f in fields:
#             raw_pvals = f.get("picklistValues", [])
#             ui_pvals = [{"label": str(v), "value": str(v)} for v in raw_pvals]
            
#             ui_fields.append({
#                 "label": f.get("FieldLabel") or f.get("label") or f.get("apiname") or f.get("name"),
#                 "name": f.get("apiname") or f.get("name"),
#                 "type": (f.get("datatype") or f.get("type", "string")).lower(),
#                 "picklistValues": ui_pvals
#             })
#         return ui_fields
#     except Exception as e:
#         logging.error(f"Error reading schema: {e}")
#         return []


# async def completion_node(state: MarketingState) -> MarketingState:
#     """
#     Final node that summarizes operations or shows proposal for review.
#     """
#     logging.info("🏁 Completing workflow...")

#     # Check if there's a final_response already set (e.g., from casual chat)
#     if state.get("final_response"):
#         logging.info("✅ Final response already set, returning")
#         state.setdefault("messages", [])
#         state["messages"].append(AIMessage(content=state["final_response"]))
#         state["current_agent"] = "completion"
#         return state

#     # Collect MCP results
#     mcp_results = state.get("mcp_results", {})
    
#     if not mcp_results:
#         state["final_response"] = "No operations were performed."
#         state.setdefault("messages", [])
#         state["messages"].append(AIMessage(content="No operations were performed."))
#         state["current_agent"] = "completion"
#         return state

#     # Check for proposal action (review mode)
#     for service_name, service_data in mcp_results.items():
#         tool_results = service_data.get("tool_results", [])
        
#         # Extract contacts from SOQL results
#         contacts_list = []
#         contact_count = 0
        
#         for result in tool_results:
#             tool_name = result.get("tool_name", "").lower()
            
#             # Extract contacts from run_dynamic_soql
#             if "run_dynamic_soql" in tool_name and result.get("status") == "success":
#                 try:
#                     response = result.get("response", {})
#                     if hasattr(response, 'content') and response.content:
#                         text_content = response.content[0].text if response.content else "[]"
#                         contacts_data = json.loads(text_content)
                        
#                         # Handle {records: [...]} or [...]
#                         records = contacts_data.get("records", []) if isinstance(contacts_data, dict) else contacts_data
                        
#                         contact_count = len(records)
#                         contacts_list = [
#                             {
#                                 "Id": c.get("Id"),
#                                 "Name": c.get("Name", "Unknown"),
#                                 "Email": c.get("Email", "")
#                             }
#                             for c in records
#                         ]
#                 except Exception as e:
#                     logging.error(f"Error extracting contacts: {e}")
            
#             # Check for proposal
#             if "propose_action" in tool_name:
#                 logging.info("📋 Proposal detected - entering review mode")
                
#                 # Extract proposal from request
#                 request = result.get("request", {})
#                 object_name = request.get("object_name", "Record")
                
#                 # Get available fields from schema for UI
#                 available_fields = get_available_fields(object_name)
                
#                 proposal_payload = {
#                     "object": object_name,
#                     "fields": [
#                         {"name": k, "value": v, "label": k}
#                         for k, v in request.get("fields", {}).items()
#                         if v is not None and str(v).strip() != ""
#                     ],
#                     "action_type": request.get("action_type", "create"),
#                     "contact_count": contact_count,
#                     "related_records": contacts_list,
#                     "available_fields": available_fields,
#                     "generated_by": "Agent"
#                 }
                
#                 # Create review message
#                 message = f"I'm ready to {proposal_payload['action_type']} the {proposal_payload['object']}."
#                 if contact_count > 0:
#                     message += f" Found {contact_count} related records."
#                 message += " Please review and confirm."
                
#                 final_json = json.dumps({
#                     "type": "review_proposal",
#                     "proposal": proposal_payload,
#                     "message": message
#                 })
                
#                 state["final_response"] = final_json
#                 state.setdefault("messages", [])
#                 state["messages"].append(AIMessage(content=final_json))
#                 state["current_agent"] = "completion"
#                 logging.info("⏸️ Pausing for proposal review")
#                 return state

#     # No proposal - generate detailed summary
#     # 🛑 SPECIAL CHECK: Did a specialized workflow already generate a final summary?
#     # EngagementWorkflow puts detailed markdown in messages[-1].content
#     # If the last message is from a specialized workflow (AIMessage) and we are not in review mode,
#     # we should arguably just use IT unless we really want to summarize everything.
    
#     # We can check if "engagement_workflow_context" is present and has data.
#     if state.get("engagement_workflow_context") and state.get("messages"):
#         # Search backwards through last 3 messages
#         found_msg = None
#         for msg in reversed(state["messages"][-3:]):
#             # Check for key phrases from the new natural language format
#             content = str(msg.content)
#             if isinstance(msg, AIMessage) and ("Good news! I found" in content or "No clicks detected" in content):
#                 found_msg = msg
#                 break
                
#         if found_msg:
#              logging.info("✅ Using specialized Engagement Workflow summary.")
#              state["final_response"] = found_msg.content
#              state["current_agent"] = "completion"
#              return state

#     llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    
#     # Extract tool results for summary
#     operations_context = []
#     for service_name, service_data in mcp_results.items():
#         tool_results = service_data.get("tool_results", [])
#         exec_summary = service_data.get("execution_summary", {})
        
#         if not tool_results:
#             continue
        
#         # Format tool results
#         formatted_results = []
#         for result in tool_results:
#             tool_name = result.get("tool_name", "Unknown")
#             status = result.get("status", "unknown")
#             request = result.get("request", {})
#             response = result.get("response", {})
            
#             # Extract record ID and name from response
#             record_id = None
#             record_name = None
#             object_name = request.get("object_name", "")
            
#             if hasattr(response, 'content') and response.content:
#                 try:
#                     response_text = response.content[0].text if response.content else ""
#                     parsed = json.loads(response_text)
                    
#                     # For upsert_salesforce_records, extract from results array
#                     if "upsert" in tool_name and parsed.get("results"):
#                         results = parsed["results"]
#                         if results and len(results) > 0:
#                             first_result = results[0]
#                             record_id = first_result.get("record_id")
                    
#                     # Get name from request
#                     if "records" in request and request["records"]:
#                         first_record = request["records"][0]
#                         fields = first_record.get("fields", {})
#                         record_name = fields.get("Name")
#                 except Exception as e:
#                     logging.debug(f"Error extracting record info: {e}")
            
#             logging.info(f"   🔍 Extracted: object={object_name}, record_id={record_id}, record_name={record_name}")
#             formatted_results.append({
#                 "tool": tool_name,
#                 "status": status,
#                 "object": object_name,
#                 "fields": request.get("fields", {}),
#                 "record_id": record_id,
#                 "record_name": record_name
#             })
        
#         operations_context.append({
#             "service": service_name,
#             "summary": exec_summary,
#             "results": formatted_results
#         })
    
#     # Build LLM prompt for summary
#     context_text = json.dumps(operations_context, indent=2)
    
#     # 🔍 Check if any operations were actually performed
#     if not operations_context or all(not ctx.get("results") for ctx in operations_context):
#         # No operations were performed - provide helpful message
#         user_goal = state.get('user_goal', 'your request')
#         final_summary = f"I couldn't perform any operations for '{user_goal}'. This might be because the request contained invalid values or missing information. Please check your request and try again with valid values."
#         logging.warning(f"⚠️ No operations performed for user goal: {user_goal}")
#     else:
#         summary_prompt = f"""Generate a friendly, natural summary of what was accomplished.

# User's Goal: {state.get('user_goal', 'N/A')}

# Operations Performed:
# {context_text}

# Instructions:
# - **CRITICAL**: Check the "status" field of each operation in the results
# - If status is "error" or if an operation has an "error" field, it FAILED - do NOT report it as successful
# - Only report operations that actually succeeded (status: "success" and no errors)
# - For FAILED operations, explain what went wrong using the error message
# - Write 1-2 sentences confirming what was ACTUALLY created/updated (not what was attempted)
# - Mention the record name naturally in the sentence (the UI will automatically make it a clickable link)
# - Include key field values (StartDate, EndDate, Status, etc.) naturally in the sentence
# - Use plain text, no markdown formatting
# - Be conversational and friendly
# - If some operations failed, be honest about it
# - **CRITICAL**: If 'Global Error' is provided below, you MUST mention it as the primary outcome.

# Global Error: {state.get('error', '')}

# Example (success): "Success! I've created the Campaign 'Summer Launch' scheduled from 2025-12-01 to 2026-01-01 with status 'Planned'."

# Example (partial failure): "I've created the Campaign 'Summer Launch', but encountered an error creating CampaignMembers: Malformed request. The Draft status may not exist for this campaign."

# Summary:"""
    
#     try:
#         response = await llm.ainvoke([HumanMessage(content=summary_prompt)])
#         final_summary = response.content.strip()
#     except Exception as e:
#         logging.error(f"Failed to generate summary: {e}")
#         # Fallback to simple summary
#         summary_parts = []
#         for service_name, service_data in mcp_results.items():
#             exec_summary = service_data.get("execution_summary", {})
#             successful = exec_summary.get("successful_calls", 0)
#             total = exec_summary.get("total_calls", 0)
#             if total > 0:
#                 summary_parts.append(f"{service_name}: {successful}/{total} operations successful")
#         final_summary = "✅ " + ", ".join(summary_parts) if summary_parts else "Workflow completed."
    
#     # ⚠️ Force-prepend critical errors if LLM missed them
#     if state.get("error") and state["error"] not in final_summary:
#         final_summary = f"⚠️ Encountered an error: {state['error']}\n\n" + final_summary
    
#     state["final_response"] = final_summary
    
#     # 🔗 Extract created records for LWC hyperlink generation
#     created_records = {}
#     for context in operations_context:
#         for result in context.get("results", []):
#             if result.get("status") == "success" and result.get("record_id") and result.get("record_name"):
#                 object_name = result.get("object", "Record")
#                 if object_name not in created_records:
#                     created_records[object_name] = []
#                 created_records[object_name].append({
#                     "Id": result["record_id"],
#                     "Name": result["record_name"]
#                 })
    
#     state["created_records"] = created_records
#     state.setdefault("messages", [])
#     state["messages"].append(AIMessage(content=final_summary))
#     state["current_agent"] = "completion"
    
#     logging.info(f"✅ Workflow completed with {len(created_records)} record types created")
#     logging.info(f"🔗 Created records for LWC: {json.dumps(created_records, indent=2)}")
#     return state



# nodes/completion.py

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
import logging
import json
import os
from agents.marketing.state import MarketingState


def get_available_fields(obj_type):
    """
    Reads schema_metadata.json to return available fields for the object.
    Returns: [{label: 'Status', name: 'Status', type: 'picklist', picklistValues: [...]}, ...]
    """
    try:
        # Go up from agents/marketing/nodes to the project root
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Path: .../agents/marketing/nodes -> .../agents/marketing -> .../agents -> .../project_root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(base_dir)))
        schema_path = os.path.join(project_root, "schema_metadata.json")
        
        # Fallback for different deployment structures
        if not os.path.exists(schema_path):
            # Try current working directory
            schema_path = os.path.join(os.getcwd(), "schema_metadata.json")
            
        if not os.path.exists(schema_path):
            # Try one level up from agents/marketing (old logic)
            schema_path = os.path.join(os.path.dirname(base_dir), "schema_metadata.json")

        if not os.path.exists(schema_path):
            logging.warning(f"⚠️ Schema file not found. Tried: {project_root}, {os.getcwd()}")
            return []
        
        with open(schema_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Find object (case-insensitive)
        obj_meta = {}
        if isinstance(data, list):
            obj_meta = next((item for item in data if item["object"].lower() == str(obj_type).lower()), {})
        else:
            obj_meta = data.get(obj_type, {})
        
        fields = obj_meta.get("fields", [])
        
        # Transform for UI
        ui_fields = []
        for f in fields:
            raw_pvals = f.get("picklistValues", [])
            ui_pvals = [{"label": str(v), "value": str(v)} for v in raw_pvals]
            
            ui_fields.append({
                "label": f.get("FieldLabel") or f.get("label") or f.get("apiname") or f.get("name"),
                "name": f.get("apiname") or f.get("name"),
                "type": (f.get("datatype") or f.get("type", "string")).lower(),
                "picklistValues": ui_pvals
            })
        return ui_fields
    except Exception as e:
        logging.error(f"Error reading schema: {e}")
        return []


def _is_engagement_workflow_summary(state: MarketingState) -> tuple[bool, str]:
    """
    Detects if the last message is from the engagement workflow.
    Uses pure logical indicators - no hardcoded phrases needed.
    
    Returns:
        (is_engagement_summary, summary_content)
    """
    ctx = state.get("engagement_workflow_context")
    if not ctx:
        return False, None
    
    # 1. Engagement workflow must have run and completed its summary step
    # The summary_node always populates 'update_summary' or 'members_who_clicked'
    has_engagement_completion = (
        "update_summary" in ctx or 
        "members_who_clicked" in ctx or
        "total_clicks_found" in ctx
    )
    
    if not has_engagement_completion:
        return False, None
    
    # 2. The last message must be from the engagement workflow's summary_node
    messages = state.get("messages", [])
    if not messages:
        return False, None
    
    last_msg = messages[-1]
    
    # Must be an AIMessage (summary_node returns AIMessage)
    if not isinstance(last_msg, AIMessage):
        return False, None
    
    content = str(last_msg.content)
    
    # 3. Validate it's a real summary (not empty/trivial)
    if content and len(content) > 50:
        logging.info("✅ Detected engagement workflow summary via context - using it directly")
        return True, content
    
    return False, None


def _handle_email_workflow_completion(state: MarketingState) -> tuple[bool, dict, dict]:
    """
    Detects if email workflow completed and returns context for LLM generation.
    Returns: (is_email, context_data, created_records_dict)
    """
    ctx = state.get("email_workflow_context")
    if not ctx or not ctx.get("campaign_id"):
        return False, {}, {}
        
    campaign_id = ctx.get("campaign_id")
    campaign_name = ctx.get("campaign_name", "Campaign")
    
    # Context for LLM
    context_data = {
        "name": campaign_name,
        "error": state.get("error")
    }

    # Inject Campaign as a 'created' record for UI linking
    records = {
        "Campaign": [{"Id": campaign_id, "Name": campaign_name}]
    }
    
    logging.info(f"✅ Detected email workflow for {campaign_name} ({campaign_id})")
    return True, context_data, records


async def completion_node(state: MarketingState) -> MarketingState:
    """
    Final node that summarizes operations or shows proposal for review.
    Intelligently detects and preserves specialized workflow summaries.
    """
    logging.info("🏁 Completing workflow...")

    # 🧹 CRITICAL STATE CLEANUP to prevent UI glitches
    if "generated_email_content" in state:
        logging.info("   🧹 Clearing persistent generated_email_content from state")
        state["generated_email_content"] = None
    


    # Check if there's a final_response already set (e.g., from casual chat)
    if state.get("final_response"):
        logging.info("✅ Final response already set, returning")
        state.setdefault("messages", [])
        state["messages"].append(AIMessage(content=state["final_response"]))
        state["current_agent"] = "completion"
        return state

    # 🎯 PRIORITY CHECK: Engagement Workflow Summary
    is_engagement, engagement_summary = _is_engagement_workflow_summary(state)
    if is_engagement:
        logging.info("🔄 Using engagement workflow summary directly")
        state["final_response"] = engagement_summary
        state["current_agent"] = "completion"
        # Don't append another message - it's already there
        return state

    # 🎯 PRIORITY CHECK: Email Workflow (AI Generated Summary)
    is_email, email_ctx, email_records = _handle_email_workflow_completion(state)
    if is_email:
        logging.info("📧 Generating AI summary for email workflow")
        
        llm = ChatOpenAI(model="gpt-4o", temperature=0.7)
        c_name = email_ctx.get("name", "Campaign")
        error_msg = email_ctx.get("error")
        
        if error_msg:
             prompt = f"The email campaign '{c_name}' encountered an error: {error_msg}. Briefly summarize this failure in natural language."
        else:
             prompt = (
                 f"The email campaign '{c_name}' was processed successfully (emails sent, Salesforce updated). "
                 "Generate a brief, natural, friendly success message. "
                 "Do NOT mention template IDs, recipient counts, or tracking details. "
                 "Do NOT say 'I have successfully processed...', just state it naturally."
             )
             
        try:
            res = await llm.ainvoke([HumanMessage(content=prompt)])
            summary = res.content.strip()
        except Exception as e:
            logging.error(f"LLM Summary failed: {e}")
            summary = f"Processed {c_name}."

        state["final_response"] = summary
        state["created_records"] = email_records # Important for LWC
        state["current_agent"] = "completion"
        state.setdefault("messages", [])
        state["messages"].append(AIMessage(content=summary))
        return state

    # Collect MCP results
    mcp_results = state.get("mcp_results", {})
    
    if not mcp_results:
        state["final_response"] = "No operations were performed."
        state.setdefault("messages", [])
        state["messages"].append(AIMessage(content="No operations were performed."))
        state["current_agent"] = "completion"
        return state

    # Check for proposal action (review mode)
    for service_name, service_data in mcp_results.items():
        tool_results = service_data.get("tool_results", [])
        
        # Extract contacts from SOQL results
        contacts_list = []
        contact_count = None  # Default to None (no search performed)
        
        for result in tool_results:
            tool_name = result.get("tool_name", "").lower()
            
            # Extract contacts from run_dynamic_soql
            if "run_dynamic_soql" in tool_name and result.get("status") == "success":
                try:
                    response = result.get("response", {})
                    if hasattr(response, 'content') and response.content:
                        text_content = response.content[0].text if response.content else "[]"
                        contacts_data = json.loads(text_content)
                        
                        # Handle {records: [...]} or [...]
                        records = contacts_data.get("records", []) if isinstance(contacts_data, dict) else contacts_data
                        
                        contact_count = len(records)
                        contacts_list = [
                            {
                                "Id": c.get("Id"),
                                "Name": c.get("Name", "Unknown"),
                                "Email": c.get("Email", "")
                            }
                            for c in records
                        ]
                except Exception as e:
                    logging.error(f"Error extracting contacts: {e}")
            
            # Check for proposal
            if "propose_action" in tool_name:
                logging.info("📋 Proposal detected - entering review mode")
                
                # Extract proposal from request
                request = result.get("request", {})
                object_name = request.get("object_name", "Record")
                
                # Get available fields from schema for UI
                available_fields = get_available_fields(object_name)
                
                proposal_payload = {
                    "object": object_name,
                    "fields": [
                        {"name": k, "value": v, "label": k}
                        for k, v in request.get("fields", {}).items()
                        if v is not None and str(v).strip() != ""
                    ],
                    "action_type": request.get("action_type", "create"),
                    "contact_count": contact_count,
                    "related_records": contacts_list,
                    "available_fields": available_fields,
                    "generated_by": "Agent"
                }
                
                # Create review message
                message = f"I'm ready to {proposal_payload['action_type']} the {proposal_payload['object']}."
                if contact_count is not None and contact_count > 0:
                    message += f" Found {contact_count} related records."
                elif contact_count == 0:
                    message += " Found 0 related records."
                message += " Please review and confirm."
                
                final_json = json.dumps({
                    "type": "review_proposal",
                    "proposal": proposal_payload,
                    "message": message
                })
                
                state["final_response"] = final_json
                state.setdefault("messages", [])
                state["messages"].append(AIMessage(content=final_json))
                state["current_agent"] = "completion"
                logging.info("⏸️ Pausing for proposal review")
                return state

    # No proposal - generate detailed summary for standard workflows
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    
    # Extract tool results for summary
    operations_context = []
    for service_name, service_data in mcp_results.items():
        tool_results = service_data.get("tool_results", [])
        exec_summary = service_data.get("execution_summary", {})
        
        if not tool_results:
            continue
        
        # Format tool results
        formatted_results = []
        for result in tool_results:
            tool_name = result.get("tool_name", "Unknown")
            status = result.get("status", "unknown")
            request = result.get("request", {})
            response = result.get("response", {})
            
            # Extract record ID and name from response
            record_id = None
            record_name = None
            object_name = request.get("object_name", "")
            
            if hasattr(response, 'content') and response.content:
                try:
                    response_text = response.content[0].text if response.content else ""
                    parsed = json.loads(response_text)
                    
                    # For upsert_salesforce_records, extract from results array
                    if "upsert" in tool_name and parsed.get("results"):
                        results = parsed["results"]
                        if results and len(results) > 0:
                            first_result = results[0]
                            record_id = first_result.get("record_id")
                    
                    # Get name from request
                    if "records" in request and request["records"]:
                        first_record = request["records"][0]
                        fields = first_record.get("fields", {})
                        record_name = fields.get("Name")
                except Exception as e:
                    logging.debug(f"Error extracting record info: {e}")
            
            logging.info(f"   🔍 Extracted: object={object_name}, record_id={record_id}, record_name={record_name}")
            formatted_results.append({
                "tool": tool_name,
                "status": status,
                "object": object_name,
                "fields": request.get("fields", {}),
                "record_id": record_id,
                "record_name": record_name
            })
        
        operations_context.append({
            "service": service_name,
            "summary": exec_summary,
            "results": formatted_results
        })
    
    # Build LLM prompt for summary
    context_text = json.dumps(operations_context, indent=2)
    
    # 🔍 Check if any operations were actually performed
    if not operations_context or all(not ctx.get("results") for ctx in operations_context):
        # No operations were performed - provide helpful message
        user_goal = state.get('user_goal', 'your request')
        final_summary = f"I couldn't perform any operations for '{user_goal}'. This might be because the request contained invalid values or missing information. Please check your request and try again with valid values."
        logging.warning(f"⚠️ No operations performed for user goal: {user_goal}")
    else:
        summary_prompt = f"""Generate a friendly, natural summary of what was accomplished.

User's Goal: {state.get('user_goal', 'N/A')}

Operations Performed:
{context_text}

Instructions:
- **CRITICAL**: Check the "status" field of each operation in the results
- If status is "error" or if an operation has an "error" field, it FAILED - do NOT report it as successful
- Only report operations that actually succeeded (status: "success" and no errors)
- For FAILED operations, explain what went wrong using the error message
- Write 1-2 sentences confirming what was ACTUALLY created/updated (not what was attempted)
- Mention the record name naturally in the sentence (the UI will automatically make it a clickable link)
- Include key field values (StartDate, EndDate, Status, etc.) naturally in the sentence
- Use plain text, no markdown formatting
- Be conversational and friendly
- If some operations failed, be honest about it
- **CRITICAL**: If 'Global Error' is provided below, you MUST mention it as the primary outcome.

Global Error: {state.get('error', '')}

Example (success): "Success! I've created the Campaign 'Summer Launch' scheduled from 2025-12-01 to 2026-01-01 with status 'Planned'."

Example (partial failure): "I've created the Campaign 'Summer Launch', but encountered an error creating CampaignMembers: Malformed request. The Draft status may not exist for this campaign."

Summary:"""
        
        try:
            response = await llm.ainvoke([HumanMessage(content=summary_prompt)])
            final_summary = response.content.strip()
        except Exception as e:
            logging.error(f"Failed to generate summary: {e}")
            # Fallback to simple summary
            summary_parts = []
            for service_name, service_data in mcp_results.items():
                exec_summary = service_data.get("execution_summary", {})
                successful = exec_summary.get("successful_calls", 0)
                total = exec_summary.get("total_calls", 0)
                if total > 0:
                    summary_parts.append(f"{service_name}: {successful}/{total} operations successful")
            final_summary = "✅ " + ", ".join(summary_parts) if summary_parts else "Workflow completed."
        
        # ⚠️ Force-prepend critical errors if LLM missed them
        if state.get("error") and state["error"] not in final_summary:
            final_summary = f"⚠️ Encountered an error: {state['error']}\n\n" + final_summary
    
    state["final_response"] = final_summary
    
    # 🔗 Extract created records for LWC hyperlink generation
    created_records = {}
    for context in operations_context:
        for result in context.get("results", []):
            # Relaxed check: allow record without explicit name (use ID as fallback)
            if result.get("status") == "success" and result.get("record_id"):
                object_name = result.get("object", "Record")
                record_name = result.get("record_name") or result.get("record_id")
                
                if object_name not in created_records:
                    created_records[object_name] = []
                created_records[object_name].append({
                    "Id": result["record_id"],
                    "Name": record_name
                })
    
    state["created_records"] = created_records
    state.setdefault("messages", [])
    state["messages"].append(AIMessage(content=final_summary))
    state["current_agent"] = "completion"
    
    logging.info(f"✅ Workflow completed with {len(created_records)} record types created")
    logging.info(f"🔗 Created records for LWC: {json.dumps(created_records, indent=2)}")
    return state