# import sys
# import logging
# import re
# import json
# from typing import Dict, Any, List
# from langgraph.graph import StateGraph, END
# from agents.marketing.state import MarketingState
# from core.helper import get_member_dependency, execute_single_tool

# # Constants
# # Constants
# BREVO_SERVICE = "Brevo MCP"
# LINKLY_SERVICE = "Linkly MCP"
# SALESFORCE_SERVICE = "Salesforce MCP"



# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("email_workflow.log", mode='a', encoding='utf-8'),
#         logging.StreamHandler(sys.stdout)
#     ],
#     force=True
# )

# logger = logging.getLogger(__name__)

# # # Configure logger for this workflow
# # logger = logging.getLogger(__name__)
# # logger.setLevel(logging.INFO)

# # # Create file handler
# # file_handler = logging.FileHandler("Email Workflow.log", mode='a', encoding='utf-8')
# # file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
# # logger.addHandler(file_handler)

# # # Add stream handler to also see in console
# # stream_handler = logging.StreamHandler(sys.stdout)
# # stream_handler.setFormatter(logging.Formatter("%(asctime)s [EMAIL_WORKFLOW] %(message)s"))
# # logger.addHandler(stream_handler)

# def _update_mcp_results(state: MarketingState, service_name: str, tool_name: str, result: Dict[str, Any]):
#     """
#     Manually update mcp_results so the Marketing Orchestrator sees the work.
#     """
#     mcp_results = state.get("mcp_results", {}) or {}
#     service_data = mcp_results.get(service_name, {"execution_summary": {}, "tool_results": []})
    
#     # Update stats
#     summary = service_data.get("execution_summary", {})
#     summary["total_calls"] = summary.get("total_calls", 0) + 1
#     if result.get("status") == "success":
#         summary["successful_calls"] = summary.get("successful_calls", 0) + 1
#     else:
#         summary["failed_calls"] = summary.get("failed_calls", 0) + 1
#     service_data["execution_summary"] = summary
        
#     # Append result
#     tool_res = {
#         "tool_name": tool_name,
#         "status": result.get("status", "unknown"),
#         "response": result.get("data", str(result)), 
#         # approximate structure for Marketing Orchestrator summarizer
#     }
#     service_data["tool_results"].append(tool_res)
    
#     # Write back
#     mcp_results[service_name] = service_data
#     state["mcp_results"] = mcp_results
#     return state

# # async def preview_template_node(state: MarketingState) -> MarketingState:
# #     """
# #     1. Previews the email template using Brevo MCP.
# #     2. Stores the preview result for link analysis.
# #     """
# #     logger.info("🚀 [EmailWorkflow] Step 1: Preview Template")
    
# #     shared_data = state.get("shared_result_sets", {})
    
# #     # Extract Campaign and Contact data
# #     campaign_id = None
# #     template_id = None
# #     contacts = []
    
# #     if "campaign" in shared_data:
# #         campaigns = shared_data["campaign"]
# #         if campaigns:
# #              campaign_data = campaigns[0]
# #              campaign_id = campaign_data.get("Id")
# #              template_id = campaign_data.get("Email_template__c")
             
# #              # Fallback if field name is different
# #              if not template_id:
# #                  template_id = campaign_data.get("description") # sometimes stored here in testing?
            
# #              # Clean field: Extract integer if format is "3 - Name"
# #              if template_id:
# #                  tid_str = str(template_id)
# #                  if not tid_str.isdigit():
# #                      # Try to match starting digits
# #                      match = re.match(r'^(\d+)', tid_str)
# #                      if match:
# #                          template_id = match.group(1)
# #                          logger.info(f"   🧹 Cleaned Template ID '{tid_str}' to '{template_id}'")
# #                      else:
# #                          logger.warning(f"   ⚠️ Could not extract integer ID from '{tid_str}'")
# #                          template_id = None
     
# #     # Also check if template_id was passed directly in the user request or previous context?
# #     # For now, rely on Salesforce data.
             
# #     if "contacts" in shared_data:
# #         contacts = shared_data["contacts"]
        
# #     logger.info(f"   Campaign ID: {campaign_id}, Template ID: {template_id}, Contacts: {len(contacts)}")

# #     if not template_id:
# #         msg = "❌ Missing Template ID in campaign data"
# #         logger.error(msg)
# #         state["error"] = msg
# #         return state

# #     if not contacts:
# #         msg = "❌ No contacts found in result set 'contacts'"
# #         logger.error(msg)
# #         state["error"] = msg
# #         return state

# #     # Sample preview for link detection (using first contact)
# #     sample_contact = contacts[0]
# #     preview_args = {
# #         "template_id": int(template_id),
# #         "recipients": [{"email": sample_contact.get("Email"), "name": sample_contact.get("FirstName")}]
# #     }

# #     try:
# #         result = await execute_single_tool(BREVO_SERVICE, "preview_email", preview_args)
        
# #         if result["status"] == "success":
# #             # Store necessary context in temporary state fields
# #             # We add them to shared_result_sets or a temporary stash?
# #             # MarketingState has 'mcp_results' which is good.
# #             # But let's use a specific key for this workflow data
# #             email_ctx = {
# #                 "template_id": int(template_id),
# #                 "contacts": contacts,
# #                 "preview_data": result["data"],
# #                 "campaign_id": campaign_id,
# #                 "campaign_name": campaign_data.get("Name")
# #             }
# #             state.setdefault("email_workflow_context", {}).update(email_ctx)
# #             logger.info("   ✅ Preview successful")
# #         else:
# #             state["error"] = f"Preview failed: {result.get('error')}"

# #     except Exception as e:
# #         logger.error(f"Failed to preview: {e}")
# #         state["error"] = str(e)

# #     return state

# async def preview_template_node(state: MarketingState) -> MarketingState:
#     """
#     1. Previews the email template using Brevo MCP.
#     2. Stores the preview result for link analysis.
#     ⚠️ STOPS workflow if template ID is missing.
#     """
#     logger.info("🚀 [EmailWorkflow] Step 1: Preview Template")
    
#     shared_data_list = state.get("shared_result_sets", []) or []
#     # 🔄 Flatten history for comprehensive lookup (ensures data from previous turns is found)
#     shared_data = {}
#     for rs in shared_data_list:
#         if rs and isinstance(rs, dict):
#             shared_data.update(rs)
            
#     logger.info(f"   Flattened shared data keys: {list(shared_data.keys())}")
    
#     # Extract Campaign and Contact data
#     campaign_id = None
#     campaign_name = "Unknown Campaign"
#     template_id = None
#     contacts = []
    
#     if "campaign" in shared_data:
#         campaigns = shared_data["campaign"]
#         if campaigns:
#              campaign_data = campaigns[0]
#              campaign_id = campaign_data.get("Id")
#              campaign_name = campaign_data.get("Name", "Unknown Campaign")
#              template_id = campaign_data.get("Email_template__c")
             
#              # Fallback if field name is different in state
#              if not template_id:
#                  template_id = campaign_data.get("description")

#     # 🔍 FALLBACK: If Template ID is missing but we have Campaign ID, try to query it from Salesforce
#     if not template_id and campaign_id:
#         logger.info(f"   🔍 Template ID missing from state. Attempting fallback query for Campaign {campaign_id}...")
#         try:
#             query = f"SELECT Id, Name, Email_template__c, description FROM Campaign WHERE Id = '{campaign_id}'"
#             res = await execute_single_tool(SALESFORCE_SERVICE, "run_dynamic_soql", {"query": query})
            
#             if res["status"] == "success":
#                 data = res["data"]
#                 records = data.get("records", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                
#                 if records:
#                     camp = records[0]
#                     template_id = camp.get("Email_template__c") or camp.get("description")
#                     campaign_name = camp.get("Name", campaign_name)
#                     logger.info(f"   ✅ Fallback query successful. Found template_id: {template_id}")
#                 else:
#                     logger.warning("   ⚠️ Fallback query returned no records.")
#             else:
#                 logger.warning(f"   ⚠️ Fallback query failed: {res.get('error')}")
#         except Exception as e:
#             logger.error(f"   ❌ Exception during fallback template query: {e}")

#     # 🧹 CLEAN: Standardize Template ID format (extract integer if format is "3 - Name")
#     if template_id:
#         tid_str = str(template_id)
#         if not tid_str.isdigit():
#             # Try to match starting digits
#             match = re.match(r'^(\d+)', tid_str)
#             if match:
#                 template_id = match.group(1)
#                 logger.info(f"   🧹 Cleaned Template ID '{tid_str}' to '{template_id}'")
#             else:
#                 logger.warning(f"   ⚠️ Could not extract integer ID from '{tid_str}'")
#                 template_id = None
     
#     if "contacts" in shared_data:
#         contacts = shared_data["contacts"]
        
#     logger.info(f"   Campaign ID: {campaign_id}, Template ID: {template_id}, Contacts: {len(contacts)}")

#     # ⛔ CRITICAL: Stop workflow if template ID is missing
#     if not template_id:
#         error_msg = (
#             f"❌ Template ID Missing\n\n"
#             f"Campaign '{campaign_name}' does not have an email template assigned.\n\n"
#             f"Action Required:\n"
#             f"1. Assign an email template to the Campaign's 'Email_template__c' field in Salesforce\n"
#             f"2. Retry sending the email\n\n"
#             f"Cannot proceed without a template."
#         )
#         logger.error(error_msg)
        
#         # Set error and mark workflow as failed
#         state["error"] = error_msg
#         state["final_response"] = error_msg  # ✅ FIX: Ensure message is shown in UI
#         state["workflow_failed"] = True  # Flag to stop execution
        
#         # ✅ FIX: Add Campaign to created_records so LWC can hyperlink it
#         state["created_records"] = {
#             "Campaign": [{
#                 "Id": campaign_id,
#                 "Name": campaign_name,
#                 "attributes": {"type": "Campaign"}
#             }]
#         }
        
#         # Update MCP results to show failure
#         state = _update_mcp_results(
#             state, 
#             BREVO_SERVICE, 
#             "preview_email", 
#             {
#                 "status": "error",
#                 "error": "Missing template ID",
#                 "data": error_msg
#             }
#         )
        
#         return state

#     # ⛔ CRITICAL: Stop workflow if no contacts found
#     if not contacts:
#         error_msg = (
#             f"❌ **No Contacts Found**\n\n"
#             f"No contacts were found in the result set for Campaign '{campaign_name}' (ID: {campaign_id}).\n\n"
#             f"**Action Required:**\n"
#             f"1. Add contacts to the campaign in Salesforce\n"
#             f"2. Retry sending the email\n\n"
#             f"**Cannot proceed without recipients.**"
#         )
#         logger.error(error_msg)
        
#         state["error"] = error_msg
#         state["final_response"] = error_msg  # ✅ FIX: Ensure message is shown in UI
#         state["workflow_failed"] = True
        
#         state = _update_mcp_results(
#             state, 
#             BREVO_SERVICE, 
#             "preview_email", 
#             {
#                 "status": "error",
#                 "error": "No contacts found",
#                 "data": error_msg
#             }
#         )
        
#         return state

#     # Sample preview for link detection (using first contact)
#     sample_contact = contacts[0]
#     logger.info(f"   👤 Sample Contact Keys: {list(sample_contact.keys())}")
    
#     # Robust name extraction
#     c_name = sample_contact.get("FirstName") or sample_contact.get("Name") or "Valued Customer"
    
#     preview_args = {
#         "template_id": int(template_id),
#         "recipients": [{"email": sample_contact.get("Email"), "name": c_name}]
#     }

#     try:
#         result = await execute_single_tool(BREVO_SERVICE, "preview_email", preview_args)
#         logger.info(f"   🛠️ Tool Result Type: {type(result)}")
        
#         if result and result["status"] == "success":
#             # Store necessary context
#             email_ctx = {
#                 "template_id": int(template_id),
#                 "contacts": contacts,
#                 "preview_data": result["data"],
#                 "campaign_id": campaign_id,
#                 "campaign_name": campaign_name
#             }
#             # Safe update to handle case where context might be None
#             ctx = state.get("email_workflow_context") or {}
#             ctx.update(email_ctx)
#             state["email_workflow_context"] = ctx
#             logger.info("   ✅ Preview successful")
#         else:
#             error_msg = f"Preview failed: {result.get('error')}"
#             state["error"] = error_msg
#             state["final_response"] = error_msg # ✅ FIX
#             state["workflow_failed"] = True

#     except Exception as e:
#         error_msg = f"Preview exception: {str(e)}"
#         logger.error(error_msg)
#         state["error"] = error_msg
#         state["final_response"] = error_msg # ✅ FIX
#         state["workflow_failed"] = True

#     return state
# async def analyze_links_node(state: MarketingState) -> MarketingState:
#     logger.info("🔍 [EmailWorkflow] Step 2: Analyzing Links")
    
#     ctx = state.get("email_workflow_context")
#     if not ctx:
#         ctx = {}
#     preview_data = ctx.get("preview_data", {})
#     logger.info(f"ctx,{ctx} and preview data: {preview_data}")
#     has_links = False
#     found_urls = []
#     template_params = set()
    
#     if preview_data and "previews" in preview_data:
#         html_content = preview_data["previews"][0].get("html_content", "")
#         # Regex to find links (both in href and in text)
#         # 1. Links in href (already have http/https)
#         href_urls = re.findall(r'href=[\'"]?(https?://[^\'" >]+)', html_content)
#         # 2. Links in text (not starting with href=)
#         # We look for http/https followed by non-space/non-bracket/non-quote characters
#         text_urls = re.findall(r'(?<!href=[\'"])(https?://[^\s<"\']+\.[^\s<"\']+)', html_content)
        
#         urls = href_urls + text_urls
        
#         # Regex to find {{ params.Name }}
#         # Common Brevo format: {{ params.FirstName }} or {{params.FirstName}}
#         # Match alphanumeric and underscores
#         found_params = re.findall(r'\{\{\s*params\.([a-zA-Z0-9_]+)\s*\}\}', html_content)
#         if found_params:
#             template_params.update(found_params)
#             logger.info(f"   📝 Found template params: {template_params}")
        
#         # Filter out unsubscribes/utility links if needed?
#         # For now, any link triggers the shortener.
#         cleaned_urls = [u for u in urls if "unsubscribe" not in u.lower()]
        
#         if cleaned_urls:
#             has_links = True
#             found_urls = list(set(cleaned_urls)) # dedupe
#             logger.info(f"   🔗 Found {len(found_urls)} unique links: {found_urls}")
    
#     ctx["has_links"] = has_links
#     ctx["found_urls"] = found_urls
#     ctx["template_params"] = list(template_params)
    
#     state["email_workflow_context"] = ctx
#     return state

# async def link_shortener_node(state: MarketingState) -> MarketingState:
#     logger.info("🔗 [EmailWorkflow] Step 3: Linkly Shortening")
    
#     ctx = state.get("email_workflow_context")
#     if not ctx:
#         ctx = {}
#     contacts = ctx.get("contacts", [])
#     found_urls = ctx.get("found_urls", []) # List of URLs to shorten
#     campaign_id = ctx.get("campaign_id")

#     if not found_urls:
#         logger.info("   No URLs to shorten.")
#         return state

#     # Prepare inputs for generate_uniqueurl
#     linkly_contacts = []
#     for c in contacts:
#         linkly_contacts.append({
#             "email": c.get("Email"),
#             "name": c.get("FirstName") or c.get("Name"),
#         })
    
#     gen_args = {
#         "campaign_id": campaign_id,
#         "contacts": linkly_contacts,
#         "urls": found_urls
#     }

#     short_links_map = {} # {contact_id: {original: {short_url, link_id}}}
    
#     try:
#         res = await execute_single_tool(LINKLY_SERVICE, "generate_uniqueurl", gen_args)
        
#         if res["status"] == "success":
#             data = res.get("data")
#             if not data:
#                 logger.error("   ❌ Link generation succeeded but returned NO DATA")
#                 return state
                
#             results = data.get("results", [])
#             logger.info(f"   ✅ Batch generation complete. Processed {len(results)} contacts.")

#             # Map back to Contact IDs (normalize to lowercase)
#             email_to_cid = {str(c.get("Email")).lower(): c.get("Id") for c in contacts if c.get("Email")}
            
#             for item in results:
#                 raw_email = item.get("contact", {}).get("email")
#                 c_email = str(raw_email).lower() if raw_email else ""
                
#                 c_id = email_to_cid.get(c_email)
                
#                 # DEBUG MAPPING
#                 if not c_id:
#                      logger.warning(f"   ⚠️ Mapping Failed: Email '{raw_email}' (normalized: '{c_email}') not found in contact list keys: {list(email_to_cid.keys())}")
                
                 
#                 if c_id:
#                     links = item.get("links", [])
#                     contact_links = {}
#                     for l in links:
#                         # Linkly tool returns dicts, check for success
#                         if l.get("status") == "success":
#                             orig = l.get("original_url")
#                             short = l.get("short_url")
#                             lid = l.get("link_id")
#                             if orig:
#                                 contact_links[orig] = {"short_url": short, "link_id": lid}
#                         else:
#                              logger.warning(f"   ⚠️ Link generation failed for {c_email}: {l.get('error')}")
                    
#                     if contact_links:
#                         short_links_map[c_id] = contact_links
#                     else:
#                         logger.warning(f"   ⚠️ No successful links for contact {c_email}")

#             ctx["short_links_map"] = short_links_map
#             state = _update_mcp_results(state, LINKLY_SERVICE, "generate_uniqueurl", res)
            
#         else:
#              logger.error(f"   ❌ Link generation failed: {res.get('error')}")
#              state["error"] = f"Link generation failed: {res.get('error')}"

#     except Exception as e:
#         logger.error(f"   ❌ Exception in link shortener: {e}")
#         state["error"] = str(e)
    
#     state["email_workflow_context"] = ctx
#     return state

# async def send_email_node(state: MarketingState) -> MarketingState:
#     logger.info("📧 [EmailWorkflow] Step 4: Sending Emails via Brevo")
    
#     ctx = state.get("email_workflow_context")
#     if not ctx:
#         ctx = {}
#     contacts = ctx.get("contacts", [])
#     template_id = ctx.get("template_id")
#     short_links_map = ctx.get("short_links_map", {})
    
#     if not contacts or not template_id:
#         return state

#     template_params = ctx.get("template_params", [])
#     logger.info(f"template_params: {template_params}")

#     # 🆕 Extract original HTML and subject if available for link substitution
#     # We use original_html because it contains the raw placeholders {{params.X}}
#     preview_data = ctx.get("preview_data", {})
#     html_content_base = None
#     email_subject = None
    
#     if preview_data and "previews" in preview_data and preview_data["previews"]:
#         first_preview = preview_data["previews"][0]
#         html_content_base = first_preview.get("original_html")
#         email_subject = first_preview.get("subject") or first_preview.get("original_subject")

#     # Prepare recipients for batch sending
#     message_versions = []
#     logger.info(f"contact email send, {len(contacts)}")

#     for contact in contacts:
#         c_id = contact.get("Id")
#         c_email = contact.get("Email")
#         if not c_email:
#             continue
            
#         c_name = contact.get("FirstName") or contact.get("Name") or "Valued Customer"
        
#         # Prepare params dynamically
#         params = {}
#         if template_params:
#             for key in template_params:
#                 val = contact.get(key)
#                 if val is None:
#                     for k, v in contact.items():
#                         if k.lower() == key.lower():
#                             val = v
#                             break
#                 if val is None and key.lower() in ["name", "firstname"]:
#                     val = c_name
#                 if val:
#                     params[key] = val
#         else:
#             params["FirstName"] = c_name

#         # Ensure consistent case for common params
#         params["Name"] = params.get("Name") or c_name
#         params["FirstName"] = params.get("FirstName") or c_name
        
#         # 🔗 LINK SUBSTITUTION LOGIC
#         contact_html = html_content_base
#         first_short_link = None

#         if c_id in short_links_map:
#             links_data = short_links_map[c_id] # {original_url: {short_url, link_id}}
            
#             for orig_url, data in links_data.items():
#                 short_url = data.get("short_url")
#                 if not short_url:
#                     continue
                
#                 if not first_short_link:
#                     first_short_link = short_url
                
#                 # 🔄 Surgical Substitution (Deduplication)
#                 if contact_html:
#                     # 1. Replace in hrefs (Shorten)
#                     contact_html = contact_html.replace(f'href="{orig_url}"', f'href="{short_url}"')
#                     contact_html = contact_html.replace(f"href='{orig_url}'", f"href='{short_url}'")
                    
#                     # 2. Case-insensitive support for {{params.Link}} if manually inserted
#                     contact_html = contact_html.replace("{{params.Link}}", short_url)
#                     contact_html = contact_html.replace("{{params.LINK}}", short_url)
#                     contact_html = contact_html.replace("{{params.link}}", short_url)

#                     # 3. Remove Text version (User Request)
#                     # We remove the long URL from text to avoid duplication with the placeholder
#                     pattern = r'(?<!href=[\'"])(' + re.escape(orig_url) + r')'
#                     contact_html = re.sub(pattern, "", contact_html)

#         # Set dedicated link params (case-insensitive support)
#         if first_short_link:
#             params["Link"] = first_short_link
#             params["LINK"] = first_short_link
#             params["link"] = first_short_link

#         # 👤 PARAMETER SUBSTITUTION LOGIC (Pre-render HTML)
#         if contact_html:
#             for key, val in params.items():
#                 if val:
#                     # Replace various common formats
#                     contact_html = contact_html.replace(f"{{{{params.{key}}}}}", str(val))
#                     contact_html = contact_html.replace(f"{{{{ params.{key} }}}}", str(val))
#                     contact_html = contact_html.replace(f"{{{{params.{key.lower()}}}}}", str(val))
#                     contact_html = contact_html.replace(f"{{{{params.{key.upper()}}}}}", str(val))

#         version = {
#             "to": [{"email": c_email, "name": c_name}],
#             "params": params
#         }
        
#         if contact_html:
#             version["htmlContent"] = contact_html
            
#         message_versions.append(version)

#     # Call Send Batch
#     # ⚠️ CRITICAL: If we are sending htmlContent per version, Brevo requires 
#     # either NO template_id OR that we omit htmlContent if using template.
#     # Since we modified the HTML for link substitution, we send as PURE HTML.
#     send_args = {
#         "message_versions": message_versions,
#         "sender_email": "aleenamathews2001@gmail.com", 
#         "sender_name": "Aleena Mathews"
#     }
    
#     # Only include template_id if NO HTML override was done
#     # (Though currently we always override if html_content_base exists)
#     if not html_content_base:
#         send_args["template_id"] = int(template_id)
    
#     # If we have a subject from preview, use it
#     if email_subject:
#         send_args["subject"] = email_subject
    
#     try:
#         res = await execute_single_tool(BREVO_SERVICE, "send_batch_emails", send_args)
#         state = _update_mcp_results(state, BREVO_SERVICE, "send_batch_emails", res)
#         if res["status"] == "success":
#             logger.info("   ✅ Batch email sent successfully")
#             send_data = res["data"]
#             # --- Parsing Logic Restored ---
#             successfully_sent_emails = set()
#             failed_sends = {}
            
#             # Parse Brevo response - handle multiple possible formats
#             if isinstance(send_data, dict):
#                 # Format 1: {"success": [...], "failed": [...]}
#                 success_list = send_data.get("success", [])
#                 failed_list = send_data.get("failed", [])
                
#                 # Format 2: {"messageIds": ["<id1>", "<id2>"], ...} - indicates all succeeded
#                 message_ids = send_data.get("messageIds", [])
                
#                 # Process success list if present
#                 if success_list:
#                     for item in success_list:
#                         email = item.get("email", "").lower() if isinstance(item, dict) else str(item).lower()
#                         if email:
#                             successfully_sent_emails.add(email)
                
#                 # Process failed list if present
#                 if failed_list:
#                     for item in failed_list:
#                         if isinstance(item, dict):
#                             email = item.get("email", "").lower()
#                             error = item.get("error", "Unknown error")
#                         else:
#                             email = str(item).lower()
#                             error = "Send failed"
                        
#                         if email:
#                             failed_sends[email] = error
#                             logger.warning(f"   ❌ Email failed for {email}: {error}")
                
#                 # Format 3: If messageIds present but no explicit success/failed lists
#                 # This means Brevo accepted all emails for sending
#                 if message_ids and not success_list and not failed_list:
#                     logger.info(f"   ℹ️ Brevo returned {len(message_ids)} messageIds - all emails accepted")
#                     # We need to map back to emails since message_versions contains them
#                     successfully_sent_emails = set([v["to"][0]["email"].lower() for v in message_versions if v.get("to")])
            
#             # Fallback: If response format is unexpected, assume all succeeded
#             if not successfully_sent_emails and not failed_sends:
#                 logger.info("   ℹ️ Brevo response format not recognized, assuming all sent successfully")
#                 successfully_sent_emails = set([v["to"][0]["email"].lower() for v in message_versions if v.get("to")])
            
#             ctx["send_result"] = send_data
#             ctx["successfully_sent_emails"] = successfully_sent_emails
#             ctx["failed_sends"] = failed_sends
            
#             logger.info(f"   📊 Parsed Send Results: {len(successfully_sent_emails)} sent, {len(failed_sends)} failed")
#         else:
#             # 🛑 ENHANCED ERROR LOGGING עבור 400 Bad Request
#             error_details = res.get("details") or res.get("error")
#             state["error"] = f"Send failed: {error_details}"
#             logger.error(f"   ❌ Send failed: {error_details}")
#             if isinstance(error_details, dict):
#                  logger.error(f"   🔍 Detailed Brevo Error: {json.dumps(error_details, indent=2)}")
#     except Exception as e:
#         state["error"] = f"Send Exception: {e}"
#         logger.error(f"   ❌ Send Exception: {e}")

#     state["email_workflow_context"] = ctx
#     return state

# async def track_delivery_status_node(state: MarketingState) -> MarketingState:
#     """
#     detects bounced emails immediately after sending using the track_email_engagement tool.
#     Bounced emails are moved from successfully_sent_emails to failed_sends.
#     """
#     logger.info("🕵️ [EmailWorkflow] Step 4.5: Checking Immediate Delivery/Bounce Status")
    
#     ctx = state.get("email_workflow_context")
#     if not ctx:
#         ctx = {}
#     successfully_sent = ctx.get("successfully_sent_emails", set())
#     failed_sends = ctx.get("failed_sends", {})
    
#     if not successfully_sent:
#         logger.info("   ℹ️ No successful sends to check.")
#         return state
        
#     # Convert set to list for API call
#     emails_to_check = list(successfully_sent)
    
#     # We call track_email_engagement
#     # It returns { "engagement": { "email": { "bounced": bool, ... } } }
    
#     try:
#         logger.info(f"   🔍 Checking status for {len(emails_to_check)} emails...")
#         res = await execute_single_tool(BREVO_SERVICE, "track_email_engagement", {"emails": emails_to_check})
        
#         if res["status"] == "success":
#             data = res["data"]
#             engagement = data.get("engagement", {})
            
#             bounced_detected = []
            
#             for email, metrics in engagement.items():
#                 # metrics might be an error dict if email invalid, or data dict
#                 if metrics.get("bounced") is True:
#                     bounced_detected.append(email)
#                     logger.warning(f"   🚨 Detected BOUNCE for {email}")
            
#             # Update lists
#             for email in bounced_detected:
#                 if email in successfully_sent:
#                     successfully_sent.remove(email)
#                     failed_sends[email] = "Detected as Bounced during immediate check"
            
#             ctx["successfully_sent_emails"] = successfully_sent
#             ctx["failed_sends"] = failed_sends
            
#             logger.info(f"   ✅ Delivery check complete. Found {len(bounced_detected)} bounces.")
#             state = _update_mcp_results(state, BREVO_SERVICE, "track_email_engagement", res)
            
#         else:
#             logger.warning(f"   ⚠️ Delivery check failed: {res.get('error')}")

#     except Exception as e:
#         logger.error(f"   ❌ Exception checking delivery status: {e}")
#         # Don't fail the workflow, just proceed with what we have
        
#     state["email_workflow_context"] = ctx
#     return state

# async def update_salesforce_node(state: MarketingState) -> MarketingState:
#     logger.info("☁️ [EmailWorkflow] Step 5: Updating Salesforce Status")
    
#     ctx = state.get("email_workflow_context")
#     if not ctx:
#         ctx = {}
#     contacts = ctx.get("contacts", [])
#     campaign_id = ctx.get("campaign_id")
#     short_links_map = ctx.get("short_links_map", {})
    
#     successfully_sent_emails = ctx.get("successfully_sent_emails", set())
#     failed_sends = ctx.get("failed_sends", {})

#     # We need to update CampaignMember status.
#     # Record structure: {CampaignId, ContactId, Status="Sent", ...}
    
#     contact_id_to_member_id = {}
#     already_has_members = False

#     # Check if contacts are actually CampaignMember objects (have ContactId)
#     if contacts and isinstance(contacts[0], dict) and contacts[0].get("ContactId"):
#          logger.info("   ℹ️ Input contacts appear to be CampaignMember records. Using existing IDs.")
#          for c in contacts:
#              c_id = c.get("ContactId")
#              m_id = c.get("Id")
#              if c_id and m_id:
#                  contact_id_to_member_id[c_id] = m_id
#          already_has_members = True

#     if not already_has_members:
#         # 1. Fetch CampaignMember IDs needed for update
#         logger.info("   🔍 Fetching CampaignMember IDs for update...")
        
#         try:
#             soql = f"SELECT Id, ContactId FROM CampaignMember WHERE CampaignId = '{campaign_id}'"
#             soql_args = {"query": soql}
            
#             current_members_res = await execute_single_tool(SALESFORCE_SERVICE, "run_dynamic_soql", soql_args)
            
#             if current_members_res["status"] == "success":
#                 data = current_members_res["data"]
#                 rows = []
                
#                 # Handle SOQL response structure (dict with 'records' or direct list)
#                 if isinstance(data, dict):
#                     rows = data.get("records", [])
#                 elif isinstance(data, list):
#                     rows = data
#                 else:
#                     logger.warning(f"   ⚠️ Unexpected SOQL result format type: {type(data)}")

#                 if rows:
#                     for row in rows:
#                         c_id = row.get("ContactId")
#                         m_id = row.get("Id")
#                         if c_id and m_id:
#                             contact_id_to_member_id[c_id] = m_id
#                     logger.info(f"   ✅ Found {len(contact_id_to_member_id)} CampaignMember records.")
#                 else:
#                      logger.warning(f"   ⚠️ No records found or unexpected format: {data}")
#             else:
#                  error_msg = current_members_res.get('error')
#                  logger.error(f"   ❌ Failed to query CampaignMembers: {error_msg}")
#                  state = _update_mcp_results(state, SALESFORCE_SERVICE, "run_dynamic_soql", current_members_res)
#                  state["error"] = error_msg
                 
#         except Exception as e:
#             logger.error(f"   ❌ Exception querying CampaignMembers: {e}")
#             state["error"] = str(e)

#     # 2. Build Upsert Payload
#     records_to_update = []
    
#     for contact in contacts:
#         c_id = contact.get("Id")
#         c_email = contact.get("Email", "").lower()
#         member_id = contact_id_to_member_id.get(c_id)
        
#         if not member_id:
#             logger.warning(f"   ⚠️ No CampaignMember found for Contact {c_id}, skipping status update.")
#             continue

#         # Logic:
#         # If email is in failed_sends -> SKIP update (leave as Draft)
#         # If email is in successfully_sent_emails -> Update to "Sent"
        
#         if c_email in failed_sends:
#             logger.info(f"   🛑 Skipping Salesforce update for bounced/failed email: {c_email}")
#             continue
            
#         if c_email not in successfully_sent_emails:
#              logger.warning(f"   ⚠️ Email {c_email} not in success list, skipping update.")
#              continue

#         fields = {
#             "Status": "Sent"
#         }
        
#         # Add link tracking data
#         if c_id in short_links_map:
#             links = short_links_map[c_id]
#             if links:
#                 first_val = list(links.values())[0]
#                 short_url = first_val.get("short_url")
#                 link_id = first_val.get("link_id")
                
#                 if short_url:
#                     fields["Link__c"] = short_url
#                 if link_id:
#                     try:
#                          fields["LinkId__c"] = float(link_id)
#                     except:
#                          fields["LinkId__c"] = link_id

#         records_to_update.append({
#             "record_id": member_id,
#             "fields": fields
#         })
#     logger.info(f"   ✅ Found {len(records_to_update)} records to update.")
#     records_to_upsert = records_to_update
#     logger.info(f"   ✅ Found {len(records_to_upsert)} records to upsert.")
        
#     if not records_to_upsert:
#         return state

#     # Batch Upsert
#     upsert_args = {
#         "object_name": "CampaignMember",
#         "records": records_to_upsert
#     }
    
#     try:
#         res = await execute_single_tool(SALESFORCE_SERVICE, "upsert_salesforce_records", upsert_args)
        
#         if res["status"] == "success":
#             raw_data = res["data"]
#             # upsert tool returns json string
#             if isinstance(raw_data, str):
#                 try:
#                     upsert_result = json.loads(raw_data)
#                 except:
#                     upsert_result = raw_data
#             else:
#                 upsert_result = raw_data
                
#             if isinstance(upsert_result, dict):
#                 if upsert_result.get("success"):
#                      logger.info(f"   ✅ Salesforce updated successfully: {upsert_result.get('successful')} ok, {upsert_result.get('failed')} failed.")
#                 else:
#                      logger.warning(f"   ⚠️ Salesforce update reported failure: {upsert_result.get('error')}")
            
#             state = _update_mcp_results(state, SALESFORCE_SERVICE, "upsert_salesforce_records", res)
#         else:
#              logger.warning(f"   ⚠️ Salesforce update warning: {res.get('error')}")
#              state = _update_mcp_results(state, SALESFORCE_SERVICE, "upsert_salesforce_records", res)
#              state["error"] = res.get('error')

#     except Exception as e:
#         logger.error(f"Salesforce update failed: {e}")
#         state["error"] = str(e)
        
#     return state


# def build_email_workflow():
#     builder = StateGraph(MarketingState)
    
#     builder.add_node("preview_template", preview_template_node)
#     builder.add_node("analyze_links", analyze_links_node)
#     builder.add_node("link_shortener", link_shortener_node)
#     builder.add_node("send_email", send_email_node)
#     builder.add_node("track_delivery", track_delivery_status_node)
#     builder.add_node("update_salesforce", update_salesforce_node)

#     builder.set_entry_point("preview_template")

#     # ✅ NEW: Check if workflow should stop after preview
#     def check_preview_success(state):
#         """Stop workflow if preview failed (e.g., missing template)"""
#         if state.get("workflow_failed"):
#             return END
#         return "analyze_links"
    
#     builder.add_conditional_edges("preview_template", check_preview_success, {
#         "analyze_links": "analyze_links",
#         END: END
#     })

#     # Conditional logic for links
#     def check_links(state):
#         if state.get("workflow_failed"):
#             return END
        
#         ctx = state.get("email_workflow_context")
#         if not ctx:
#             return "send_email"
#         result = ctx.get("has_links", False)
#         return "link_shortener" if result else "send_email"

#     builder.add_conditional_edges("analyze_links", check_links, {
#         "link_shortener": "link_shortener",
#         "send_email": "send_email",
#         END: END
#     })
    
#     builder.add_edge("link_shortener", "send_email")
#     builder.add_edge("send_email", "track_delivery")
#     builder.add_edge("track_delivery", "update_salesforce")
#     builder.add_edge("update_salesforce", END)

#     return builder.compile()
# # def build_email_workflow():
# #     builder = StateGraph(MarketingState)
    
# #     builder.add_node("preview_template", preview_template_node)
# #     builder.add_node("analyze_links", analyze_links_node)
# #     builder.add_node("link_shortener", link_shortener_node)
# #     builder.add_node("send_email", send_email_node)
# #     builder.add_node("track_delivery", track_delivery_status_node)
# #     builder.add_node("update_salesforce", update_salesforce_node)

# #     builder.set_entry_point("preview_template")

# #     # Conditional logic
# #     def check_links(state):
# #         ctx = state.get("email_workflow_context")
# #         if not ctx:
# #             return "send_email"
# #         result = ctx.get("has_links", False)
# #         return "link_shortener" if result else "send_email"

# #     builder.add_conditional_edges("analyze_links", check_links, {
# #         "link_shortener": "link_shortener",
# #         "send_email": "send_email"
# #     })
    
# #     builder.add_edge("preview_template", "analyze_links")
# #     builder.add_edge("link_shortener", "send_email")
# #     builder.add_edge("send_email", "track_delivery")
# #     builder.add_edge("track_delivery", "update_salesforce")
# #     builder.add_edge("update_salesforce", END)

# #     return builder.compile()
import sys
import logging
import re
import json
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from agents.marketing.state import MarketingState
from core.helper import get_member_dependency, execute_single_tool

# Constants
# Constants
BREVO_SERVICE = "Brevo MCP"
LINKLY_SERVICE = "Linkly MCP"
SALESFORCE_SERVICE = "Salesforce MCP"



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("email_workflow.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger(__name__)

# # Configure logger for this workflow
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# # Create file handler
# file_handler = logging.FileHandler("Email Workflow.log", mode='a', encoding='utf-8')
# file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
# logger.addHandler(file_handler)

# # Add stream handler to also see in console
# stream_handler = logging.StreamHandler(sys.stdout)
# stream_handler.setFormatter(logging.Formatter("%(asctime)s [EMAIL_WORKFLOW] %(message)s"))
# logger.addHandler(stream_handler)

def _update_mcp_results(state: MarketingState, service_name: str, tool_name: str, result: Dict[str, Any]):
    """
    Manually update mcp_results so the Marketing Orchestrator sees the work.
    """
    mcp_results = state.get("mcp_results", {}) or {}
    service_data = mcp_results.get(service_name, {"execution_summary": {}, "tool_results": []})
    
    # Update stats
    summary = service_data.get("execution_summary", {})
    summary["total_calls"] = summary.get("total_calls", 0) + 1
    if result.get("status") == "success":
        summary["successful_calls"] = summary.get("successful_calls", 0) + 1
    else:
        summary["failed_calls"] = summary.get("failed_calls", 0) + 1
    service_data["execution_summary"] = summary
        
    # Append result
    tool_res = {
        "tool_name": tool_name,
        "status": result.get("status", "unknown"),
        "response": result.get("data", str(result)), 
        # approximate structure for Marketing Orchestrator summarizer
    }
    service_data["tool_results"].append(tool_res)
    
    # Write back
    mcp_results[service_name] = service_data
    state["mcp_results"] = mcp_results
    return state

 


async def preview_template_node(state: MarketingState) -> MarketingState:
    """
    1. Previews the email template using Brevo MCP.
    2. Stores the preview result for link analysis.
    ⚠️ STOPS workflow if template ID is missing.
    """
    logger.info("🚀 [EmailWorkflow] Step 1: Preview Template")
    
    shared_data_list = state.get("shared_result_sets", []) or []
    # 🔄 Flatten history for comprehensive lookup (ensures data from previous turns is found)
    shared_data = {}
    for rs in shared_data_list:
        if rs and isinstance(rs, dict):
            shared_data.update(rs)
            
    logger.info(f"   Flattened shared data keys: {list(shared_data.keys())}")
    
    # Extract Campaign and Contact data
    campaign_id = None
    campaign_name = "Unknown Campaign"
    template_id = None
    contacts = []
    
    if "campaign" in shared_data:
        campaigns = shared_data["campaign"]
        if campaigns:
             campaign_data = campaigns[0]
             campaign_id = campaign_data.get("Id")
             campaign_name = campaign_data.get("Name", "Unknown Campaign")
             template_id = campaign_data.get("Email_template__c")
             
             # Fallback if field name is different in state
             if not template_id:
                 template_id = campaign_data.get("description")

    # 🔍 FALLBACK: If Template ID is missing but we have Campaign ID, try to query it from Salesforce
    if not template_id and campaign_id:
        logger.info(f"   🔍 Template ID missing from state. Attempting fallback query for Campaign {campaign_id}...")
        try:
            query = f"SELECT Id, Name, Email_template__c, description FROM Campaign WHERE Id = '{campaign_id}'"
            res = await execute_single_tool(SALESFORCE_SERVICE, "run_dynamic_soql", {"query": query})
            
            if res["status"] == "success":
                data = res["data"]
                records = data.get("records", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                
                if records:
                    camp = records[0]
                    template_id = camp.get("Email_template__c") or camp.get("description")
                    campaign_name = camp.get("Name", campaign_name)
                    logger.info(f"   ✅ Fallback query successful. Found template_id: {template_id}")
                else:
                    logger.warning("   ⚠️ Fallback query returned no records.")
            else:
                logger.warning(f"   ⚠️ Fallback query failed: {res.get('error')}")
        except Exception as e:
            logger.error(f"   ❌ Exception during fallback template query: {e}")

    # 🧹 CLEAN: Standardize Template ID format (extract integer if format is "3 - Name")
    if template_id:
        tid_str = str(template_id)
        if not tid_str.isdigit():
            # Try to match starting digits
            match = re.match(r'^(\d+)', tid_str)
            if match:
                template_id = match.group(1)
                logger.info(f"   🧹 Cleaned Template ID '{tid_str}' to '{template_id}'")
            else:
                logger.warning(f"   ⚠️ Could not extract integer ID from '{tid_str}'")
                template_id = None
     
    if "contacts" in shared_data:
        contacts = shared_data["contacts"]
        
    logger.info(f"   Campaign ID: {campaign_id}, Template ID: {template_id}, Contacts: {len(contacts)}")

    # ⛔ CRITICAL: Stop workflow if template ID is missing
    if not template_id:
        error_msg = (
            f"❌ Template ID Missing\n\n"
            f"Campaign '{campaign_name}' does not have an email template assigned.\n\n"
            f"Action Required:\n"
            f"1. Assign an email template to the Campaign's 'Email_template__c' field in Salesforce\n"
            f"2. Retry sending the email\n\n"
            f"Cannot proceed without a template."
        )
        logger.error(error_msg)
        
        # Set error and mark workflow as failed
        state["error"] = error_msg
        state["final_response"] = error_msg  # ✅ FIX: Ensure message is shown in UI
        state["workflow_failed"] = True  # Flag to stop execution
        
        # ✅ FIX: Add Campaign to created_records so LWC can hyperlink it
        state["created_records"] = {
            "Campaign": [{
                "Id": campaign_id,
                "Name": campaign_name,
                "attributes": {"type": "Campaign"}
            }]
        }
        
        # Update MCP results to show failure
        state = _update_mcp_results(
            state, 
            BREVO_SERVICE, 
            "preview_email", 
            {
                "status": "error",
                "error": "Missing template ID",
                "data": error_msg
            }
        )
        
        return state

    # ⛔ CRITICAL: Stop workflow if no contacts found
    if not contacts:
        error_msg = (
            f"❌ **No Contacts Found**\n\n"
            f"No contacts were found in the result set for Campaign '{campaign_name}' (ID: {campaign_id}).\n\n"
            f"**Action Required:**\n"
            f"1. Add contacts to the campaign in Salesforce\n"
            f"2. Retry sending the email\n\n"
            f"**Cannot proceed without recipients.**"
        )
        logger.error(error_msg)
        
        state["error"] = error_msg
        state["final_response"] = error_msg  # ✅ FIX: Ensure message is shown in UI
        state["workflow_failed"] = True
        
        state = _update_mcp_results(
            state, 
            BREVO_SERVICE, 
            "preview_email", 
            {
                "status": "error",
                "error": "No contacts found",
                "data": error_msg
            }
        )
        
        return state

    # Sample preview for link detection (using first contact)
    sample_contact = contacts[0]
    logger.info(f"   👤 Sample Contact Keys: {list(sample_contact.keys())}")
    
    # Robust name extraction
    c_name = sample_contact.get("FirstName") or sample_contact.get("Name") or "Valued Customer"
    
    preview_args = {
        "template_id": int(template_id),
        "recipients": [{"email": sample_contact.get("Email"), "name": c_name}]
    }

    try:
        result = await execute_single_tool(BREVO_SERVICE, "preview_email", preview_args)
        logger.info(f"   🛠️ Tool Result Type: {type(result)}")
        
        if result and result["status"] == "success":
            # Store necessary context
            email_ctx = {
                "template_id": int(template_id),
                "contacts": contacts,
                "preview_data": result["data"],
                "campaign_id": campaign_id,
                "campaign_name": campaign_name
            }
            # Safe update to handle case where context might be None
            ctx = state.get("email_workflow_context") or {}
            ctx.update(email_ctx)
            state["email_workflow_context"] = ctx
            logger.info("   ✅ Preview successful")
        else:
            error_msg = f"Preview failed: {result.get('error')}"
            state["error"] = error_msg
            state["final_response"] = error_msg # ✅ FIX
            state["workflow_failed"] = True

    except Exception as e:
        error_msg = f"Preview exception: {str(e)}"
        logger.error(error_msg)
        state["error"] = error_msg
        state["final_response"] = error_msg # ✅ FIX
        state["workflow_failed"] = True

    return state

async def analyze_links_node(state: MarketingState) -> MarketingState:
    logger.info("🔍 [EmailWorkflow] Step 2: Analyzing Links")
    
    ctx = state.get("email_workflow_context")
    if not ctx:
        ctx = {}
    preview_data = ctx.get("preview_data", {})
    logger.info(f"ctx,{ctx} and preview data: {preview_data}")
    has_links = False
    found_urls = []
    template_params = set()
        
    if preview_data and "previews" in preview_data:
        html_content = preview_data["previews"][0].get("html_content", "")
        # Regex to find links
        # Looking for href="http..." or https...
        import re
        # urls = re.findall(r'href=[\'"]?(https?://[^\'" >]+)', html_content)
        
        href_urls = re.findall(r'href=[\'"]?(https?://[^\'" >]+)', html_content)
        # 2. Links in text (not starting with href=)
        # We look for http/https followed by non-space/non-bracket/non-quote characters
        text_urls = re.findall(r'(?<!href=[\'"])(https?://[^\s<"\']+\.[^\s<"\']+)', html_content)
        
        urls = href_urls + text_urls
        # Regex to find {{ params.Name }}
        # Common Brevo format: {{ params.FirstName }} or {{params.FirstName}}
        # Match alphanumeric and underscores
        found_params = re.findall(r'\{\{\s*params\.([a-zA-Z0-9_]+)\s*\}\}', html_content)
        if found_params:
            template_params.update(found_params)
            logger.info(f"   📝 Found template params: {template_params}")
        
        # Filter out unsubscribes/utility links if needed?
        # For now, any link triggers the shortener.
        cleaned_urls = [u for u in urls if "unsubscribe" not in u.lower()]
        
        if cleaned_urls:
            has_links = True
            found_urls = list(set(cleaned_urls)) # dedupe
            logger.info(f"   🔗 Found {len(found_urls)} unique links: {found_urls}")
    
    ctx["has_links"] = has_links
    ctx["found_urls"] = found_urls
    ctx["template_params"] = list(template_params)
    
    state["email_workflow_context"] = ctx
    return state

async def link_shortener_node(state: MarketingState) -> MarketingState:
    logger.info("🔗 [EmailWorkflow] Step 3: Linkly Shortening")
    
    ctx = state.get("email_workflow_context")
    if not ctx:
        ctx = {}
    contacts = ctx.get("contacts", [])
    found_urls = ctx.get("found_urls", []) # List of URLs to shorten
    campaign_id = ctx.get("campaign_id")

    if not found_urls:
        logger.info("   No URLs to shorten.")
        return state

    # Prepare inputs for generate_uniqueurl
    linkly_contacts = []
    for c in contacts:
        linkly_contacts.append({
            "email": c.get("Email"),
            "name": c.get("FirstName") or c.get("Name"),
        })
    
    gen_args = {
        "campaign_id": campaign_id,
        "contacts": linkly_contacts,
        "urls": found_urls
    }

    short_links_map = {} # {contact_id: {original: {short_url, link_id}}}
    
    try:
        res = await execute_single_tool(LINKLY_SERVICE, "generate_uniqueurl", gen_args)
        
        if res["status"] == "success":
            data = res["data"]
            results = data.get("results", [])
            logger.info(f"   ✅ Batch generation complete. Processed {len(results)} contacts.")

            # 🛑 DEBUG: Log full Linkly response
            logger.info(f"   🐛 Linkly Response Data: {json.dumps(data, indent=2)}")
            
            # Map back to Contact IDs (normalize to lowercase)
            email_to_cid = {str(c.get("Email")).lower(): c.get("Id") for c in contacts if c.get("Email")}
            
            for item in results:
                raw_email = item.get("contact", {}).get("email")
                c_email = str(raw_email).lower() if raw_email else ""
                
                c_id = email_to_cid.get(c_email)
                
                # DEBUG MAPPING
                if not c_id:
                     logger.warning(f"   ⚠️ Mapping Failed: Email '{raw_email}' (normalized: '{c_email}') not found in contact list keys: {list(email_to_cid.keys())}")
                
                 
                if c_id:
                    links = item.get("links", [])
                    contact_links = {}
                    for l in links:
                        # Linkly tool returns dicts, check for success
                        if l.get("status") == "success":
                            orig = l.get("original_url")
                            short = l.get("short_url")
                            lid = l.get("link_id")
                            if orig:
                                contact_links[orig] = {"short_url": short, "link_id": lid}
                        else:
                             logger.warning(f"   ⚠️ Link generation failed for {c_email}: {l.get('error')}")
                    
                    if contact_links:
                        short_links_map[c_id] = contact_links
                    else:
                        logger.warning(f"   ⚠️ No successful links for contact {c_email}")

            ctx["short_links_map"] = short_links_map
            state = _update_mcp_results(state, LINKLY_SERVICE, "generate_uniqueurl", res)
            
        else:
             logger.error(f"   ❌ Link generation failed: {res.get('error')}")
             state["error"] = f"Link generation failed: {res.get('error')}"

    except Exception as e:
        logger.error(f"   ❌ Exception in link shortener: {e}")
        state["error"] = str(e)
    
    state["email_workflow_context"] = ctx
    return state

async def send_email_node(state: MarketingState) -> MarketingState:
    logger.info("📧 [EmailWorkflow] Step 4: Sending Emails via Brevo")
    
    ctx = state.get("email_workflow_context")
    if not ctx:
        ctx = {}
    contacts = ctx.get("contacts", [])
    template_id = ctx.get("template_id")
    short_links_map = ctx.get("short_links_map", {})
    
    if not contacts or not template_id:
        return state

    # template_params = ctx.get("template_params", [])
    # logger.info(f"template_params: {template_params}")
    # # Prepare recipients for batch sending
    # recipients = []
    # logger.info(f"contact email send ,{len(contacts)}")
    # for contact in contacts:
    #     c_id = contact.get("Id")
    #     c_email = contact.get("Email")
    #     # Robust name extraction
    #     c_name = contact.get("FirstName") or contact.get("Name") or "Valued Customer"
        
    #     # Prepare params dynamically
    #     params = {}
    #     if template_params:
    #         for key in template_params:
    #             # 1. Exact match
    #             val = contact.get(key)
    #             # 2. Case-insensitive match
    #             if val is None:
    #                 for k, v in contact.items():
    #                     if k.lower() == key.lower():
    #                         val = v
    #                         break
    #             if val:
    #                 params[key] = val
    #     else:
    #         # Fallback for backward compatibility
    #         params["FirstName"] = c_name
    #         params["FirstName "] = c_name 

    template_params = ctx.get("template_params", [])
    logger.info(f"template_params: {template_params}")

    # Prepare recipients for batch sending
    recipients = []
    logger.info(f"contact email send, {len(contacts)}")

    for contact in contacts:
        c_id = contact.get("Id")
        c_email = contact.get("Email")
        
        # Extract name from various possible fields
        c_name = contact.get("FirstName") or contact.get("Name") or "Valued Customer"
        
        # Prepare params dynamically
        params = {}
        
        if template_params:
            for key in template_params:
                val = None
                
                # 1. Exact match
                val = contact.get(key)
                
                # 2. Case-insensitive match if exact match not found
                if val is None:
                    for k, v in contact.items():
                        if k.lower() == key.lower():
                            val = v
                            break
                
                # 3. Special handling for name-related parameters
                # If template asks for "Name" or "FirstName", use c_name
                if val is None and key.lower() in ["name", "firstname"]:
                    val = c_name
                
                if val:
                    params[key] = val
        else:
            # Fallback for backward compatibility
            params["FirstName"] = c_name
        
        # Ensure both Name and FirstName are set if either exists in contact
        # This handles cases where template might use either parameter
        if "Name" in contact or "FirstName" in contact:
            if "Name" not in params:
                params["Name"] = c_name
            if "FirstName" not in params:
                params["FirstName"] = c_name
        
        logger.info(f"Contact {c_id} params: {params}")
        
        # Add to recipients (you'll need to add the actual recipient dict here)
        # recipients.append({"email": c_email, "params": params, ...})

        
        # Inject short links if available
        if c_id in short_links_map:
            links = short_links_map[c_id]
            # links is { original_url: {short_url, link_id} } or empty
            
            if links:
                # Get first link data
                first_val = list(links.values())[0]
                if isinstance(first_val, dict):
                    short_url = first_val.get("short_url")
                else:
                    short_url = first_val # Fallback
                
                if short_url:
                    params["LINK"] = short_url
        
        recipient = {
            "email": c_email,
            "name": c_name or "", 
            "params": params
        }
        recipients.append(recipient)

    # Call Send Batch
    send_args = {
        "template_id": int(template_id),
        "recipients": recipients,
        "sender_email": "aleenamathews2001@gmail.com", 
        "sender_name": "Aleena Mathews"
    }
    
    try:
        res = await execute_single_tool(BREVO_SERVICE, "send_batch_emails", send_args)
        if res["status"] == "success":
            logger.info("   ✅ Batch email sent successfully")
            send_data = res["data"]
            
            # --- Parsing Logic Restored ---
            successfully_sent_emails = set()
            failed_sends = {}
            
            # Parse Brevo response - handle multiple possible formats
            if isinstance(send_data, dict):
                # Format 1: {"success": [...], "failed": [...]}
                success_list = send_data.get("success", [])
                failed_list = send_data.get("failed", [])
                
                # Format 2: {"messageIds": ["<id1>", "<id2>"], ...} - indicates all succeeded
                message_ids = send_data.get("messageIds", [])
                
                # Process success list if present
                if success_list:
                    for item in success_list:
                        email = item.get("email", "").lower() if isinstance(item, dict) else str(item).lower()
                        if email:
                            successfully_sent_emails.add(email)
                
                # Process failed list if present
                if failed_list:
                    for item in failed_list:
                        if isinstance(item, dict):
                            email = item.get("email", "").lower()
                            error = item.get("error", "Unknown error")
                        else:
                            email = str(item).lower()
                            error = "Send failed"
                        
                        if email:
                            failed_sends[email] = error
                            logger.warning(f"   ❌ Email failed for {email}: {error}")
                
                # Format 3: If messageIds present but no explicit success/failed lists
                # This means Brevo accepted all emails for sending
                if message_ids and not success_list and not failed_list:
                    logger.info(f"   ℹ️ Brevo returned {len(message_ids)} messageIds - all emails accepted")
                    # We need to map back to emails since messageIds don't contain them
                    successfully_sent_emails = set([r["email"].lower() for r in recipients if r.get("email")])
            
            # Fallback: If response format is unexpected, assume all succeeded
            if not successfully_sent_emails and not failed_sends:
                logger.info("   ℹ️ Brevo response format not recognized, assuming all sent successfully")
                successfully_sent_emails = set([r["email"].lower() for r in recipients if r.get("email")])
            
            ctx["send_result"] = send_data
            ctx["successfully_sent_emails"] = successfully_sent_emails
            ctx["failed_sends"] = failed_sends
            
            logger.info(f"   📊 Parsed Send Results: {len(successfully_sent_emails)} sent, {len(failed_sends)} failed")
            
            state = _update_mcp_results(state, BREVO_SERVICE, "send_batch_emails", res)
        else:
            state["error"] = f"Send failed: {res.get('error')}"
            logger.error(f"   ❌ Send failed: {res.get('error')}")
    except Exception as e:
        state["error"] = f"Send Exception: {e}"
        logger.error(f"   ❌ Send Exception: {e}")

    state["email_workflow_context"] = ctx
    return state

async def track_delivery_status_node(state: MarketingState) -> MarketingState:
    """
    detects bounced emails immediately after sending using the track_email_engagement tool.
    Bounced emails are moved from successfully_sent_emails to failed_sends.
    """
    logger.info("🕵️ [EmailWorkflow] Step 4.5: Checking Immediate Delivery/Bounce Status")
    
    ctx = state.get("email_workflow_context")
    if not ctx:
        ctx = {}
    successfully_sent = ctx.get("successfully_sent_emails", set())
    failed_sends = ctx.get("failed_sends", {})
    
    if not successfully_sent:
        logger.info("   ℹ️ No successful sends to check.")
        return state
        
    # Convert set to list for API call
    emails_to_check = list(successfully_sent)
    
    # We call track_email_engagement
    # It returns { "engagement": { "email": { "bounced": bool, ... } } }
    
    try:
        logger.info(f"   🔍 Checking status for {len(emails_to_check)} emails...")
        res = await execute_single_tool(BREVO_SERVICE, "track_email_engagement", {"emails": emails_to_check})
        
        if res["status"] == "success":
            data = res["data"]
            engagement = data.get("engagement", {})
            
            bounced_detected = []
            
            for email, metrics in engagement.items():
                # metrics might be an error dict if email invalid, or data dict
                if metrics.get("bounced") is True:
                    bounced_detected.append(email)
                    logger.warning(f"   🚨 Detected BOUNCE for {email}")
            
            # Update lists
            for email in bounced_detected:
                if email in successfully_sent:
                    successfully_sent.remove(email)
                    failed_sends[email] = "Detected as Bounced during immediate check"
            
            ctx["successfully_sent_emails"] = successfully_sent
            ctx["failed_sends"] = failed_sends
            
            logger.info(f"   ✅ Delivery check complete. Found {len(bounced_detected)} bounces.")
            state = _update_mcp_results(state, BREVO_SERVICE, "track_email_engagement", res)
            
        else:
            logger.warning(f"   ⚠️ Delivery check failed: {res.get('error')}")

    except Exception as e:
        logger.error(f"   ❌ Exception checking delivery status: {e}")
        # Don't fail the workflow, just proceed with what we have
        
    state["email_workflow_context"] = ctx
    return state

async def update_salesforce_node(state: MarketingState) -> MarketingState:
    logger.info("☁️ [EmailWorkflow] Step 5: Updating Salesforce Status")
    
    ctx = state.get("email_workflow_context")
    if not ctx:
        ctx = {}
    contacts = ctx.get("contacts", [])
    campaign_id = ctx.get("campaign_id")
    short_links_map = ctx.get("short_links_map", {})
    
    successfully_sent_emails = ctx.get("successfully_sent_emails", set())
    failed_sends = ctx.get("failed_sends", {})

    # We need to update CampaignMember status.
    # Record structure: {CampaignId, ContactId, Status="Sent", ...}
    
    contact_id_to_member_id = {}
    already_has_members = False

    # Check if contacts are actually CampaignMember objects (have ContactId)
    if contacts and isinstance(contacts[0], dict) and contacts[0].get("ContactId"):
         logger.info("   ℹ️ Input contacts appear to be CampaignMember records. Using existing IDs.")
         for c in contacts:
             c_id = c.get("ContactId")
             m_id = c.get("Id")
             if c_id and m_id:
                 contact_id_to_member_id[c_id] = m_id
         already_has_members = True

    if not already_has_members:
        # 1. Fetch CampaignMember IDs needed for update
        logger.info("   🔍 Fetching CampaignMember IDs for update...")
        
        try:
            soql = f"SELECT Id, ContactId FROM CampaignMember WHERE CampaignId = '{campaign_id}'"
            soql_args = {"query": soql}
            
            current_members_res = await execute_single_tool(SALESFORCE_SERVICE, "run_dynamic_soql", soql_args)
            
            if current_members_res["status"] == "success":
                data = current_members_res["data"]
                rows = []
                
                # Handle SOQL response structure (dict with 'records' or direct list)
                if isinstance(data, dict):
                    rows = data.get("records", [])
                elif isinstance(data, list):
                    rows = data
                else:
                    logger.warning(f"   ⚠️ Unexpected SOQL result format type: {type(data)}")

                if rows:
                    for row in rows:
                        c_id = row.get("ContactId")
                        m_id = row.get("Id")
                        if c_id and m_id:
                            contact_id_to_member_id[c_id] = m_id
                    logger.info(f"   ✅ Found {len(contact_id_to_member_id)} CampaignMember records.")
                else:
                     logger.warning(f"   ⚠️ No records found or unexpected format: {data}")
            else:
                 error_msg = current_members_res.get('error')
                 logger.error(f"   ❌ Failed to query CampaignMembers: {error_msg}")
                 state = _update_mcp_results(state, SALESFORCE_SERVICE, "run_dynamic_soql", current_members_res)
                 state["error"] = error_msg
                 
        except Exception as e:
            logger.error(f"   ❌ Exception querying CampaignMembers: {e}")
            state["error"] = str(e)

    # 2. Build Upsert Payload
    records_to_update = []
    
    for contact in contacts:
        c_id = contact.get("Id")
        c_email = contact.get("Email", "").lower()
        member_id = contact_id_to_member_id.get(c_id)
        
        if not member_id:
            logger.warning(f"   ⚠️ No CampaignMember found for Contact {c_id}, skipping status update.")
            continue

        # Logic:
        # If email is in failed_sends -> SKIP update (leave as Draft)
        # If email is in successfully_sent_emails -> Update to "Sent"
        
        if c_email in failed_sends:
            logger.info(f"   🛑 Skipping Salesforce update for bounced/failed email: {c_email}")
            continue
            
        if c_email not in successfully_sent_emails:
             logger.warning(f"   ⚠️ Email {c_email} not in success list, skipping update.")
             continue

        fields = {
            "Status": "Sent"
        }
        
        # Add link tracking data
        if c_id in short_links_map:
            links = short_links_map[c_id]
            if links:
                first_val = list(links.values())[0]
                short_url = first_val.get("short_url")
                link_id = first_val.get("link_id")
                
                if short_url:
                    fields["Link__c"] = short_url
                if link_id:
                    try:
                         fields["LinkId__c"] = float(link_id)
                    except:
                         fields["LinkId__c"] = link_id

        records_to_update.append({
            "record_id": member_id,
            "fields": fields
        })
    logger.info(f"   ✅ Found {len(records_to_update)} records to update.")
    records_to_upsert = records_to_update
    logger.info(f"   ✅ Found {len(records_to_upsert)} records to upsert.")
        
    if not records_to_upsert:
        return state

    # Batch Upsert
    upsert_args = {
        "object_name": "CampaignMember",
        "records": records_to_upsert
    }
    
    try:
        res = await execute_single_tool(SALESFORCE_SERVICE, "upsert_salesforce_records", upsert_args)
        
        if res["status"] == "success":
            raw_data = res["data"]
            # upsert tool returns json string
            if isinstance(raw_data, str):
                try:
                    upsert_result = json.loads(raw_data)
                except:
                    upsert_result = raw_data
            else:
                upsert_result = raw_data
                
            if isinstance(upsert_result, dict):
                if upsert_result.get("success"):
                     logger.info(f"   ✅ Salesforce updated successfully: {upsert_result.get('successful')} ok, {upsert_result.get('failed')} failed.")
                else:
                     logger.warning(f"   ⚠️ Salesforce update reported failure: {upsert_result.get('error')}")
            
            state = _update_mcp_results(state, SALESFORCE_SERVICE, "upsert_salesforce_records", res)
        else:
             logger.warning(f"   ⚠️ Salesforce update warning: {res.get('error')}")
             state = _update_mcp_results(state, SALESFORCE_SERVICE, "upsert_salesforce_records", res)
             state["error"] = res.get('error')

    except Exception as e:
        logger.error(f"Salesforce update failed: {e}")
        state["error"] = str(e)
        
    return state


def build_email_workflow():
    builder = StateGraph(MarketingState)
    
    builder.add_node("preview_template", preview_template_node)
    builder.add_node("analyze_links", analyze_links_node)
    builder.add_node("link_shortener", link_shortener_node)
    builder.add_node("send_email", send_email_node)
    builder.add_node("track_delivery", track_delivery_status_node)
    builder.add_node("update_salesforce", update_salesforce_node)

    builder.set_entry_point("preview_template")

    # ✅ NEW: Check if workflow should stop after preview
    def check_preview_success(state):
        """Stop workflow if preview failed (e.g., missing template)"""
        if state.get("workflow_failed"):
            return END
        return "analyze_links"
    
    builder.add_conditional_edges("preview_template", check_preview_success, {
        "analyze_links": "analyze_links",
        END: END
    })

    # Conditional logic for links
    def check_links(state):
        if state.get("workflow_failed"):
            return END
        
        ctx = state.get("email_workflow_context")
        if not ctx:
            return "send_email"
        result = ctx.get("has_links", False)
        return "link_shortener" if result else "send_email"

    builder.add_conditional_edges("analyze_links", check_links, {
        "link_shortener": "link_shortener",
        "send_email": "send_email",
        END: END
    })
    
    builder.add_edge("link_shortener", "send_email")
    builder.add_edge("send_email", "track_delivery")
    builder.add_edge("track_delivery", "update_salesforce")
    builder.add_edge("update_salesforce", END)

    return builder.compile()
# def build_email_workflow():
#     builder = StateGraph(MarketingState)
    
#     builder.add_node("preview_template", preview_template_node)
#     builder.add_node("analyze_links", analyze_links_node)
#     builder.add_node("link_shortener", link_shortener_node)
#     builder.add_node("send_email", send_email_node)
#     builder.add_node("track_delivery", track_delivery_status_node)
#     builder.add_node("update_salesforce", update_salesforce_node)

#     builder.set_entry_point("preview_template")

#     # Conditional logic
#     def check_links(state):
#         ctx = state.get("email_workflow_context")
#         if not ctx:
#             return "send_email"
#         result = ctx.get("has_links", False)
#         return "link_shortener" if result else "send_email"

#     builder.add_conditional_edges("analyze_links", check_links, {
#         "link_shortener": "link_shortener",
#         "send_email": "send_email"
#     })
    
#     builder.add_edge("preview_template", "analyze_links")
#     builder.add_edge("link_shortener", "send_email")
#     builder.add_edge("send_email", "track_delivery")
#     builder.add_edge("track_delivery", "update_salesforce")
#     builder.add_edge("update_salesforce", END)

#     return builder.compile()