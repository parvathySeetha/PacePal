# from typing import List, Dict, Any, Optional
# from Error.sf_error import SalesforceApiError
# from client.sf_client import SalesforceClient
# from baseagent import resolve_placeholders,call_llm,fetch_prompt_metadata
# import logging
# import json
# import asyncio
# import sys
# from dotenv import load_dotenv
# import os
 

# load_dotenv()
# from openai import AsyncOpenAI
# from chromadbutils import ChromaDBManager, chroma_client, schema_data, ensure_schema_initialized

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("TEST_CLIENT2.log", mode='a', encoding='utf-8'),
#         logging.StreamHandler()
#     ],
#     force=True
# )
# chroma_manager = None

# def get_chroma_manager():
#     global chroma_manager
#     if not chroma_manager:
#         chroma_manager = ChromaDBManager(chroma_client)
#     return chroma_manager

# sf_client = SalesforceClient("agent")

 
# openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# _sf_connected = False

# def ensure_sf_connected():
#     """Ensure Salesforce is connected (call this at runtime, not import time)"""
#     global _sf_connected
#     if not _sf_connected:
#         _sf_connected = sf_client.connect()
#     return _sf_connected


# def find_relevant_objects_and_fields(user_query: str, context_object: str = None):
#     """Find relevant objects and fields using ChromaDB with detailed logging"""
#     global schema_data
    
#     # 💤 LAZY INIT TRIGGER
#     ensure_schema_initialized()
#     # Reload schema_data if it was None (since init might have populated it)
#     import chromadbutils
#     if hasattr(chromadbutils, 'schema_data') and chromadbutils.schema_data:
#         schema_data = chromadbutils.schema_data

#     logging.debug(f"Received user_query: '{user_query}' with context_object: '{context_object}'")

#     if not user_query or not user_query.strip():
#         logging.warning("Empty user query provided")
#         return None, None

#     # schema_data might still be None if init failed or DB empty, but we allow trying
#     if not schema_data:
#         # Try one last re-fetch from the module
#         if hasattr(chromadbutils, 'schema_data'):
#              schema_data = chromadbutils.schema_data
        
#         if not schema_data:
#             logging.error("Schema data not initialized")
#             return None, None

#     try:
#         # Stage 1: Object search
#         logging.debug("Searching for relevant objects...")
#         cm = get_chroma_manager()
#         object_results = cm.search_objects(user_query, top_k=2)
#         logging.debug(f"Object results: {object_results}")

#         if not object_results:

#             logging.info("No relevant objects found")
#             return None, None

#         selected_object = object_results[0]['object_name']
#         logging.info(f"Most relevant object: {selected_object}")

#         # Stage 2: Field search
#         logging.debug(f"Searching fields in selected object: {selected_object} using query: '{user_query}'")
#         field_results = cm.search_fields(selected_object, user_query, top_k=10)
#         logging.debug(f"Initial field results: {field_results}")
        
#         logging.debug(f"Field results after fallback: {field_results}")

#         if field_results:
#             top_field = field_results[0]
#             logging.info(f"Top matched field: {top_field['field_name']} (distance: {top_field['distance']})")

#             logging.debug(f"Checking for 'Name' field on {selected_object}")
#             name_field_results = cm.search_fields(selected_object, "Name", top_k=1)
#             has_name = len(name_field_results) > 0 and name_field_results[0]['field_name'] == 'Name'
#             logging.debug(f"Has 'Name' field: {has_name}")

#             # Build selected fields with full metadata
#             selected_fields = ['Id']
#             selected_field_metadata = []  # Store full field info for prompt injection
            
#             if has_name:
#                 selected_fields.append('Name')

#             if top_field['field_name'] not in selected_fields:
#                 selected_fields.append(top_field['field_name'])
#                 selected_field_metadata.append(top_field)  # Store full metadata

#             logging.debug(f"Initial selected fields: {selected_fields}")

#             # Add additional fields under threshold
#             distance_threshold = 1.3
#             max_fields = 8

#             for field_result in field_results[1:]:
#                 if (field_result['distance'] < distance_threshold and
#                         field_result['field_name'] not in selected_fields and
#                         len(selected_fields) < max_fields):
#                     selected_fields.append(field_result['field_name'])
#                     selected_field_metadata.append(field_result)  # Store full metadata
#                     logging.debug(f"Added field: {field_result['field_name']} (distance: {field_result['distance']})")

#             # 💉 FORCE-INCLUDE email_template__c for Campaigns
#             # This allows Brevo to pick up the template ID dynamically
#             if selected_object.lower() == 'campaign' and 'Email_template__c' not in selected_fields:
#                  selected_fields.append('Email_template__c')
#                  logging.info("💉 Force-added 'Email_template__c' to selected fields for Campaign")

#             relevant_field = {
#                 "object": selected_object,
#                 "fields": selected_fields,
#                 "field_metadata": selected_field_metadata,  # ✅ NEW: Store all field descriptions
#                 "filter_field": top_field['field_name'],
#                 "description": top_field['description'],
#                 "datatype": top_field['datatype'],
#                 "context_object": context_object
#             }

#             logging.info(f"Final selected object: {selected_object}")
#             logging.info(f"Final selected fields: {selected_fields}")
#             logging.info(f"Field metadata count: {len(selected_field_metadata)}")
#             return selected_object, relevant_field

#         logging.info("No relevant fields found")
#         return selected_object, None

#     except Exception as e:
#         logging.error(f"Error finding objects/fields: {e}")
#         return None, None

# async def generate_structured_response(
#     user_query: str,
#     selected_object: Optional[str],
#     relevant_fields: Optional[Dict[str, Any]],
#     context: Optional[Dict[str, Any]] = None
# ) -> str:
#     """Generate structured JSON response using LLM via call_llm helper"""
#     try:
#         # 🔍 CRITICAL DEBUG: Log what we're working with
#         logging.info(f"🔍 generate_structured_response called with:")
#         logging.info(f"   - user_query: {user_query[:100]}...")
#         logging.info(f"   - selected_object: {selected_object}")
#         logging.info(f"   - relevant_fields: {relevant_fields}")
        
#         prompt_metadata = fetch_prompt_metadata("Salesforce CRUD JSON Prompt")
        
         
#         logging.info(f"Prompt metadata: {prompt_metadata}")
        
#         resolved_prompt = resolve_placeholders(
#             prompt=prompt_metadata["prompt"],
#             configs=prompt_metadata["configs"],
#             state=context
#         )
        
#         logging.info(f"Resolved prompt: {resolved_prompt}")


#         # 🔑 INJECT FIELD DESCRIPTIONS FROM CHROMADB
#         # This prevents LLM from hallucinating fields and provides context for each field
#         field_context = ""
#         if relevant_fields and relevant_fields.get('field_metadata'):
#             # Build comprehensive field information for ALL fields found by ChromaDB
#             field_descriptions = []
            
#             # Add descriptions for all fields that ChromaDB found
#             for field_meta in relevant_fields.get('field_metadata', []):
#                 field_descriptions.append(f"""
# **{field_meta.get('field_name')}**:
# - Data Type: {field_meta.get('datatype', 'Unknown')}
# - Description: {field_meta.get('description', 'No description available')}
# """)
            
#             field_context = f"""

# ⚠️ CRITICAL SCHEMA INFORMATION FOR {selected_object} OBJECT:

# The following field information comes from the actual Salesforce schema (found via semantic search).
# DO NOT use fields that are not listed here or in your general knowledge of standard Salesforce fields.

# {' '.join(field_descriptions)}

# **Available Fields for {selected_object}:**
# {', '.join(relevant_fields.get('fields', []))}

# ⚠️ DO NOT HALLUCINATE FIELDS: Only use fields that are explicitly listed above or are standard Salesforce fields you know exist.

# ⚠️ IMPORTANT: The field descriptions above explain HOW to use each field. Pay special attention to date fields and their filtering logic.
# """

#             resolved_prompt += field_context
#             logging.info(f"✅ Injected {len(field_descriptions)} field descriptions into prompt for {selected_object}")
            
#         # ✅ All other prompt content is now in Salesforce template
#         # The code below builds ONLY the dynamic placeholders that get injected
        
#         logging.info(f"About to call LLM...")
        
#         model = "gpt-4o"
#         provider = "OpenAI"
        
#         # ADD SESSION CONTEXT to user query if available (FULLY GENERIC)
#         session_context = context.get("session_context", {}) if context else {}
#         shared_result_sets = context.get("shared_result_sets", {}) if context else {}
#         session_info = ""
        
#         # EXTRACT FROM SHARED_RESULT_SETS (from SOQL queries and upsert operations)
#         if shared_result_sets:
#             for result_name, records in shared_result_sets.items():
#                 if records and isinstance(records, list):
#                     record_list = "\n".join([
#                         f"- {r.get('Name', r.get('Id', 'Unnamed'))} (ID: {r.get('Id')})" 
#                         for r in records[-50:]  # Show last 50 of each type
#                     ])
#                     session_info += f"\n\nAVAILABLE {result_name.upper()} DATA:\n{record_list}"
        
#         # EXTRACT FROM SESSION_CONTEXT.CREATED_RECORDS (from session persistence)
#         if session_context:
#             created_records = session_context.get("created_records", {})
#             conversation_history = session_context.get("conversation_history", [])
            
#             # Dynamically show ALL created object types (Campaign, Contact, Lead, etc.)
#             if created_records:
#                 for obj_type, records in created_records.items():
#                     if records:
#                         record_list = "\n".join([
#                             f"- {r.get('Name', 'Unnamed')} (ID: {r.get('Id')})" 
#                             for r in records[-50:]  # Show last 50 of each type
#                         ])
#                         session_info += f"\n\nPREVIOUSLY CREATED {obj_type.upper()}(S) IN THIS SESSION:\n{record_list}"
            
#             if conversation_history:
#                 last_action = conversation_history[-1] if conversation_history else {}
#                 if last_action:
#                     session_info += f"\n\nPREVIOUS REQUEST: {last_action.get('user_goal', '')}"
        
        
#         # FETCH NEED VALUE FIELDS for defaults
#         need_value_fields = []
#         objects_to_check = []
        
#         if selected_object:
#             objects_to_check.append(selected_object)
            
#         # 🧠 SMART CHECK: If "Campaign" is in query but not selected (e.g. "Find contacts and create campaign")
#         if "campaign" in user_query.lower() and "campaign" not in [o.lower() for o in objects_to_check]:
#              logging.info("🧠 Detailed query involves Campaign, fetching its defaults too.")
#              objects_to_check.append("Campaign")

#         # 🧠 SMART CHECK: If "CampaignMember" is in query (e.g. from Proceed message)
#         if "campaignmember" in user_query.lower().replace(" ", "") and "CampaignMember" not in objects_to_check:
#              logging.info("🧠 Detailed query involves CampaignMember, fetching its defaults too.")
#              objects_to_check.append("CampaignMember")

#         # 🧠 AUTO-ADD CampaignMember when creating Campaign with contacts
#         if "Campaign" in objects_to_check:
#             # Check if contacts are involved (either in query or in session context)
#             has_contacts = ("contact" in user_query.lower() or 
#                           "member" in user_query.lower() or
#                           (session_context and session_context.get("created_records", {}).get("Contact")))
            
#             if has_contacts and "CampaignMember" not in objects_to_check:
#                 logging.info("🧠 Campaign creation involves contacts, auto-adding CampaignMember for status setup")
#                 objects_to_check.append("CampaignMember")


#         if objects_to_check:
#             try:
#                 cm = get_chroma_manager()
#                 default_value_map = {}
#                 for obj in objects_to_check:
#                     fields_metadata = cm.get_need_value_fields(obj)
#                     for f in fields_metadata:
#                         fname = f.get('field_name')
#                         if fname:
#                             need_value_fields.append(fname)
#                             if f.get('defaultValue'):
#                                 default_value_map[fname] = f.get('defaultValue')
                    
#                 need_value_fields = list(set(need_value_fields)) # Deduplicate
#                 logging.info(f"🔍 Combined need value fields: {need_value_fields}")
#                 logging.info(f"🔍 Default Values found: {default_value_map}")
#             except Exception as e:
#                 logging.warning(f"⚠️ Failed to fetch need value fields: {e}")
#         else:
#              logging.warning(f"⚠️ No objects to check for defaults")

#         logging.info(f"Session info: {session_info}")
        
#         # Enhance user query with session information and SMART DEFAULTS instruction
#         enhanced_user_query = user_query
#         defaults_instruction = ""  # ✅ Initialize to prevent UnboundLocalError
        
#         if need_value_fields:
#             defaults_list = ", ".join(need_value_fields)
            
#             # Dynamic Default Logic
#             import datetime
#             today = datetime.date.today()
            
#             # Helper to evaluate default string expressions like "Today + 7 days"
#             def evaluate_default(val_str):
#                 try:
#                     val_lower = str(val_str).lower()
#                     if "today" in val_lower:
#                         parts = val_lower.split("+")
#                         base = today
#                         days_add = 0
#                         if len(parts) > 1 and "day" in parts[1]:
#                             import re
#                             nums = re.findall(r'\d+', parts[1])
#                             if nums:
#                                 days_add = int(nums[0])
                        
#                         return str(base + datetime.timedelta(days=days_add))
                    
#                     if "startdate" in val_lower:
#                         # Cannot evaluate dependent fields easily here without complicated parsing
#                         # But we can pass the instruction to the LLM
#                         return val_str 
                        
#                     return val_str
#                 except:
#                     return val_str

#             defaults_instruction = f"""
# \n\n⚠️ MANDATORY SCHEMA DEFAULTS - "NeedValue" Fields:
# The following fields are marked as Recommended in the schema: [{defaults_list}].

# 🔴 CRITICAL: You MUST include these default values in your JSON output if user didn't specify otherwise:
# """
#             # Generic Loop for Defaults
#             # Safeguard: Ensure map exists (it should from prior logic)
#             local_defaults_map = locals().get('default_value_map', {})
            
#             for field, raw_default in local_defaults_map.items():
#                 evaluated_default = evaluate_default(raw_default)
#                 defaults_instruction += f"""
#    - {field}: REQUIRED - Use '{evaluated_default}' if user didn't specify.
#      Example: {{"{field}": "{evaluated_default}"}}
# """
            
#             # ✅ All CampaignMember rules and examples are now in Salesforce template
            
#             enhanced_user_query += defaults_instruction
#         else:
#              defaults_instruction = ""
        
#         # 🔑 TEMPLATE SELECTION LOGIC - REMOVED per user request
            
#         if session_info:
#             enhanced_user_query = f"{user_query}{session_info}" + defaults_instruction
            
#         enhanced_user_query += f"\n\n⚠️ IMPORTANT: If the user refers to 'this <object>', 'the <object>', or uses pronouns, use the ID from the session context above."
#         enhanced_user_query += f"\n\n⚠️ DATA SOURCE WARNING: The session context above shows RECENT records (up to 50). Do NOT assume this is the complete list. ALways prefer querying via relationships (e.g. `WHERE CampaignId = '...'`) instead of listing IDs manually (`WHERE Id IN (...)`)."

#         # 🔍 DEBUG: Log the enhanced query to verify defaults are included
#         logging.info(f"📋 Enhanced User Query Length: {len(enhanced_user_query)} chars")
#         if "StartDate" in enhanced_user_query:
#             logging.info("✅ StartDate default instruction FOUND in enhanced query")
#         else:
#             logging.warning("❌ StartDate default instruction NOT FOUND in enhanced query")
        
#         # Add timeout
#         try:
#             raw_response = await asyncio.wait_for(
#                 call_llm(
#                     system_prompt=resolved_prompt,
#                     user_prompt=enhanced_user_query,  # 🔑 Use enhanced query with session context
#                     default_model=model,
#                     default_provider=provider,
#                     default_temperature=0.0,
#                 ),
#                   timeout=30.0
#             )
#         except asyncio.TimeoutError:
#             logging.error("❌ LLM call timed out after 30 seconds")
#             raise
        
#         logging.info(f"✅ LLM call completed")
#         logging.info(f"🤖 LLM response (raw): {raw_response}")
#         # Normalize the response content
#         # LangChain's response.content can be str, list, or other types
#         if isinstance(raw_response, str):
#             content = raw_response.strip()
#         elif isinstance(raw_response, list):
#             # Sometimes content is a list of content blocks
#             content = " ".join(str(block) for block in raw_response).strip()
#         else:
#             content = str(raw_response).strip()
        
#         # Remove markdown code fences if present
#         if content.startswith("```json"):
#             content = content[7:]
#         elif content.startswith("```"):
#             content = content[3:]
        
#         if content.endswith("```"):
#             content = content[:-3]
        
#         content = content.strip()
        
#         logging.info(f"🤖 Cleaned content: {content}")
        
#         # Validate JSON
#         try:
#             parsed = json.loads(content)
#             logging.info(f"✅ Successfully parsed JSON: {parsed}")
            
#             return content
            
#         except json.JSONDecodeError as e:
#             logging.error(f"❌ Invalid JSON from LLM: {e}")
#             logging.error(f"Content that failed to parse: {content}")
#             return json.dumps({
#                 "type": "error",
#                 "message": "Failed to generate valid JSON response",
#                 "uiType": "ErrorMessage"
#             })
    
#     except Exception as e:
#         logging.error(f"❌ Error generating response: {e}", exc_info=True)
#         return json.dumps({
#             "type": "error",
#             "message": f"LLM error: {str(e)}",
#             "uiType": "ErrorMessage"
#         })

# async def generate_all_toolinput(
#     query: str,
#     context: Optional[Dict[str, Any]] = None
# ) -> Dict[str, Any]:
#     """
#     Main generate tool function for MCP server generate input for all the other salesforce tools need to be the inital tool
#     """
#     try:
#         # Ensure SF connection (happens at runtime, not import)
#         if not ensure_sf_connected():
#             return {
#                 "json_response": json.dumps({
#                     "type": "error",
#                     "message": "Salesforce connection failed",
#                     "uiType": "ErrorMessage"
#                 }),
#                 "context": context
#             }
        
#         logging.info(f"Generate tool called with query: {query}")
#         logging.info(f"Generate tool called with context: {context}")
        
#         # ✅ NEW: Extract task directive and pending updates
#         task_directive = None
#         pending_updates = None
#         context_object = None
        
#         if context and isinstance(context, dict):
#             task_directive = context.get("task_directive")
#             pending_updates = context.get("pending_updates")
            
#             if task_directive:
#                 logging.info(f"📋 Task Directive: {task_directive}")
#             if pending_updates:
#                 logging.info(f"📌 Pending Updates: {pending_updates}")
            
#             # Extract context object hint
#             context_object = context.get("object") or context.get("Object")
        
#         # ✅ NEW: If task directive is about updating CampaignMember status, use it as the query
#         if task_directive and "CampaignMember" in task_directive and "status" in task_directive.lower():
#             logging.info("🎯 Detected CampaignMember status update directive, using as query")
#             query = task_directive
            
#             # Provide context object hint to help find_relevant_objects_and_fields
#             context_object = "CampaignMember"
            
#             # Also add pending updates info to the query for better context
#             if pending_updates:
#                 contact_ids = pending_updates.get("contact_ids", [])
#                 campaign_id = pending_updates.get("campaign_id")
#                 if contact_ids:
#                     logging.info(f"📧 Found {len(contact_ids)} contacts to update for campaign {campaign_id}")
#                     # The query will guide the LLM to generate update operations
        
#         # Find relevant objects and fields
#         selected_object, relevant_fields = find_relevant_objects_and_fields(
#             query,
#             context_object
#         )
        
#         if not selected_object:
#             return {
#                 "json_response": json.dumps({
#                     "type": "unsupported",
#                     "reason": "Could not identify relevant Salesforce object",
#                     "summary": "Unable to process this request"
#                 }),
#                 "context": context
#             }
        
#         # Generate structured response
#         response_content = await generate_structured_response(
#             query,
#             selected_object,
#             relevant_fields,
#             context
#         )
        
#         return {
#             "json_response": response_content
#         }
    
#     except Exception as e:
#         logging.error(f"Error in generate_query: {e}")
#         return {
#             "json_response": json.dumps({
#                 "type": "error",
#                 "message": f"Generate tool error: {str(e)}",
#                 "uiType": "ErrorMessage"
#             }),
#             "context": context
#         }
#--------------------------final-------------

# from typing import List, Dict, Any, Optional
# from Error.sf_error import SalesforceApiError
# from client.sf_client import SalesforceClient
# from baseagent import resolve_placeholders, call_llm, fetch_prompt_metadata
# import logging
# import json
# import asyncio
# import sys
# from dotenv import load_dotenv
# import os

# load_dotenv()
# from openai import AsyncOpenAI
# from chromadbutils import ChromaDBManager, chroma_client, schema_data, ensure_schema_initialized

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.FileHandler("TEST_CLIENT2.log", mode='a', encoding='utf-8'),
#         logging.StreamHandler()
#     ],
#     force=True
# )

# chroma_manager = None

# def get_chroma_manager():
#     global chroma_manager
#     if not chroma_manager:
#         chroma_manager = ChromaDBManager(chroma_client)
#     return chroma_manager

# sf_client = SalesforceClient("agent")
# openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# _sf_connected = False

# def ensure_sf_connected():
#     """Ensure Salesforce is connected (call this at runtime, not import time)"""
#     global _sf_connected
#     if not _sf_connected:
#         _sf_connected = sf_client.connect()
#     return _sf_connected


# def get_object_relationships():
#     """
#     Dynamically extract object relationships from schema_data
#     Returns dict of junction objects and their related objects
    
#     Schema format: List of dicts with structure:
#     [{"object": "ObjectName", "fields": [{"apiname": "FieldName", ...}]}]
#     """
#     global schema_data
#     if not schema_data:
#         return {}
    
#     relationships = {}
    
#     # Handle both dict and list formats
#     if isinstance(schema_data, list):
#         # Schema is a list of object definitions
#         for obj_def in schema_data:
#             obj_name = obj_def.get('object')
#             if not obj_name:
#                 continue
                
#             fields = obj_def.get('fields', [])
#             related_objects = []
            
#             for field in fields:
#                 # Check both 'apiname' and 'fieldapiname' (different schema formats)
#                 field_name = field.get('apiname') or field.get('fieldapiname', '')
                
#                 # If field ends with 'Id' and isn't just 'Id', it's a relationship
#                 if field_name.endswith('Id') and field_name != 'Id':
#                     related_obj = field_name[:-2]  # Remove 'Id' suffix
#                     if related_obj:
#                         related_objects.append(related_obj)
            
#             if related_objects:
#                 relationships[obj_name] = related_objects
    
#     elif isinstance(schema_data, dict):
#         # Schema is a dict with object names as keys
#         for obj_name, obj_data in schema_data.items():
#             fields = obj_data.get('fields', [])
#             related_objects = []
            
#             for field in fields:
#                 field_name = field.get('apiname') or field.get('fieldapiname', '')
#                 if field_name.endswith('Id') and field_name != 'Id':
#                     related_obj = field_name[:-2]
#                     if related_obj:
#                         related_objects.append(related_obj)
            
#             if related_objects:
#                 relationships[obj_name] = related_objects
    
#     logging.debug(f"📊 Extracted relationships: {relationships}")
#     return relationships


# def find_relevant_objects_and_fields(user_query: str, context_object: str = None):
#     """
#     Find relevant objects and fields using ChromaDB - fully dynamic multi-object detection
#     """
#     global schema_data
    
#     # 💤 LAZY INIT TRIGGER
#     ensure_schema_initialized()
#     import chromadbutils
#     if hasattr(chromadbutils, 'schema_data') and chromadbutils.schema_data:
#         schema_data = chromadbutils.schema_data

#     logging.info(f"🔍 Processing query: '{user_query[:100]}...'")
#     logging.debug(f"Context object hint: {context_object}")

#     if not user_query or not user_query.strip():
#         logging.warning("Empty user query provided")
#         return None, None

#     if not schema_data:
#         if hasattr(chromadbutils, 'schema_data'):
#             schema_data = chromadbutils.schema_data
#         if not schema_data:
#             logging.error("Schema data not initialized")
#             return None, None

#     try:
#         cm = get_chroma_manager()
        
#         # 🔑 STEP 1: Semantic search for potentially relevant objects
#         object_results = cm.search_objects(user_query, top_k=5)
        
#         if not object_results:
#             logging.info("No relevant objects found")
#             return None, None
        
#         logging.info(f"🔍 Semantic search found {len(object_results)} objects:")
#         for idx, obj in enumerate(object_results):
#             logging.info(f"   {idx+1}. {obj['object_name']} (distance: {obj.get('distance', 'N/A'):.3f})")
        
#         # 🔑 STEP 2: Analyze query structure to detect action types
#         query_lower = user_query.lower()
        
#         action_verbs = {
#             "find": ["find", "get", "fetch", "search", "list", "show", "select", "query"],
#             "create": ["create", "new", "add", "make", "insert"],
#             "update": ["update", "change", "modify", "edit", "set"],
#             "delete": ["delete", "remove"],
#             "assign": ["assign", "add to", "link", "connect", "attach"]
#         }
        
#         detected_actions = set()
#         for action_type, verbs in action_verbs.items():
#             if any(verb in query_lower for verb in verbs):
#                 detected_actions.add(action_type)
        
#         logging.info(f"🎯 Detected actions: {list(detected_actions)}")
        
#         # 🔑 STEP 3: Determine primary object based on context and actions
#         RELEVANCE_THRESHOLD = 1.5
#         relevant_objects = [
#             obj for obj in object_results 
#             if obj.get('distance', 999) < RELEVANCE_THRESHOLD
#         ]
        
#         logging.info(f"📦 Objects under threshold ({RELEVANCE_THRESHOLD}): {[obj['object_name'] for obj in relevant_objects]}")
        
#         # Priority logic for primary object selection
#         primary_object = None
        
#         if context_object:
#             # Explicit context object takes highest priority
#             primary_object = context_object
#             logging.info(f"🎯 Using explicit context_object: {primary_object}")
#         elif "find" in detected_actions or "search" in detected_actions:
#             # For queries with find/search, prioritize the first relevant object
#             if relevant_objects:
#                 primary_object = relevant_objects[0]['object_name']
#                 logging.info(f"🎯 Primary object (for filtering): {primary_object}")
#         else:
#             # Default to semantic ranking
#             primary_object = object_results[0]['object_name']
#             logging.info(f"🎯 Primary object (semantic): {primary_object}")
        
#         # 🔑 STEP 4: Collect secondary objects
#         secondary_objects = []
#         for obj in relevant_objects:
#             obj_name = obj['object_name']
#             if obj_name != primary_object:
#                 secondary_objects.append(obj_name)
        
#         logging.info(f"📦 Secondary objects: {secondary_objects}")
        
#         # 🔑 STEP 5: Infer junction objects based on relationships
#         objects_to_process = [primary_object] + secondary_objects
#         objects_set = set(objects_to_process)
        
#         # Get dynamic relationships from schema
#         object_relationships = get_object_relationships()
        
#         # Find junction objects that connect our objects
#         for junction_obj, related_objs in object_relationships.items():
#             if junction_obj in objects_set:
#                 continue  # Already included
            
#             # Check if this junction object connects 2+ of our objects
#             connections = set(related_objs) & objects_set
#             if len(connections) >= 2:
#                 logging.info(f"🧠 Inferred junction object: {junction_obj} (connects {connections})")
#                 objects_to_process.append(junction_obj)
        
#         # 🔑 STEP 6: Collect field metadata for ALL relevant objects
#         all_objects_metadata = {}
        
#         logging.info(f"📦 Processing field metadata for {len(objects_to_process)} objects")
        
#         for obj_name in objects_to_process:
#             try:
#                 logging.debug(f"  Searching fields in {obj_name}...")
#                 field_results = cm.search_fields(obj_name, user_query, top_k=10)
                
#                 if not field_results:
#                     logging.warning(f"  ⚠️ No field results for {obj_name}")
#                     continue
                
#                 top_field = field_results[0]
#                 logging.info(f"  ✓ {obj_name} - Top field: {top_field['field_name']} (distance: {top_field['distance']:.3f})")
                
#                 # Check for Name field
#                 name_field_results = cm.search_fields(obj_name, "Name", top_k=1)
#                 has_name = (len(name_field_results) > 0 and 
#                            name_field_results[0]['field_name'] == 'Name')
                
#                 # Build selected fields
#                 selected_fields = ['Id']
#                 selected_field_metadata = []
                
#                 if has_name:
#                     selected_fields.append('Name')
                
#                 if top_field['field_name'] not in selected_fields:
#                     selected_fields.append(top_field['field_name'])
#                     selected_field_metadata.append(top_field)
                
#                 # Add additional relevant fields
#                 distance_threshold = 1.3
#                 max_fields = 8
                
#                 for field_result in field_results[1:]:
#                     if (field_result['distance'] < distance_threshold and
#                             field_result['field_name'] not in selected_fields and
#                             len(selected_fields) < max_fields):
#                         selected_fields.append(field_result['field_name'])
#                         selected_field_metadata.append(field_result)
                
#                 # 💉 Add commonly needed fields based on object type
#                 # This can be extended or loaded from schema metadata
#                 common_fields = {
#                     "campaign": ["Status", "StartDate", "EndDate", "Email_template__c"],
#                     "contact": ["Email", "FirstName", "LastName"],
#                     "campaignmember": ["Status", "ContactId", "CampaignId"],
#                     "account": ["Type", "Industry"],
#                     "lead": ["Status", "Email", "Company"]
#                 }
                
#                 obj_lower = obj_name.lower()
#                 if obj_lower in common_fields:
#                     for common_field in common_fields[obj_lower]:
#                         if common_field not in selected_fields:
#                             selected_fields.append(common_field)
#                             logging.debug(f"    💉 Added common field: {common_field}")
                
#                 # Store metadata
#                 all_objects_metadata[obj_name] = {
#                     "object": obj_name,
#                     "fields": selected_fields,
#                     "field_metadata": selected_field_metadata,
#                     "filter_field": top_field['field_name'],
#                     "description": top_field['description'],
#                     "datatype": top_field['datatype']
#                 }
                
#                 logging.info(f"  ✅ {obj_name} - {len(selected_fields)} fields, {len(selected_field_metadata)} with metadata")
                
#             except Exception as e:
#                 logging.error(f"  ❌ Error processing {obj_name}: {e}")
#                 continue
        
#         # 🔑 STEP 7: Return primary object with all metadata
#         if not all_objects_metadata:
#             logging.warning("⚠️ No field metadata collected for any object")
#             return primary_object, None
        
#         if primary_object in all_objects_metadata:
#             primary_metadata = all_objects_metadata[primary_object]
#         else:
#             # Fallback to first available object
#             primary_object = list(all_objects_metadata.keys())[0]
#             primary_metadata = all_objects_metadata[primary_object]
#             logging.warning(f"⚠️ Using fallback primary object: {primary_object}")
        
#         # Add multi-object metadata
#         primary_metadata["all_objects"] = all_objects_metadata
#         primary_metadata["context_object"] = context_object
        
#         logging.info(f"✅ Returning: primary={primary_object}, total_objects={len(all_objects_metadata)}")
#         return primary_object, primary_metadata

#     except Exception as e:
#         logging.error(f"❌ Error finding objects/fields: {e}", exc_info=True)
#         return None, None


# async def generate_structured_response(
#     user_query: str,
#     selected_object: Optional[str],
#     relevant_fields: Optional[Dict[str, Any]],
#     context: Optional[Dict[str, Any]] = None
# ) -> str:
#     """Generate structured JSON response using LLM - multi-object aware"""
#     try:
#         logging.info(f"🔍 generate_structured_response called")
#         logging.info(f"   - Primary object: {selected_object}")
#         logging.info(f"   - Query: {user_query[:100]}...")
        
#         prompt_metadata = fetch_prompt_metadata("Salesforce CRUD JSON Prompt")
        
#         resolved_prompt = resolve_placeholders(
#             prompt=prompt_metadata["prompt"],
#             configs=prompt_metadata["configs"],
#             state=context
#         )

#         # 🔑 INJECT FIELD DESCRIPTIONS (MULTI-OBJECT SUPPORT)
#         field_context = ""
#         all_objects = relevant_fields.get('all_objects', {}) if relevant_fields else {}
        
#         if all_objects:
#             # 🎯 MULTI-OBJECT MODE
#             logging.info(f"🎯 Multi-object mode: Injecting fields for {len(all_objects)} objects")
            
#             field_context = "\n\n" + "="*60 + "\n"
#             field_context += "⚠️ CRITICAL SCHEMA INFORMATION FOR MULTIPLE OBJECTS\n"
#             field_context += "="*60 + "\n\n"
#             field_context += "The following field information comes from the actual Salesforce schema.\n"
#             field_context += "DO NOT use fields that are not listed here.\n\n"
            
#             for obj_name, obj_meta in all_objects.items():
#                 field_context += f"\n{'─'*60}\n"
#                 field_context += f"📦 OBJECT: {obj_name}\n"
#                 field_context += f"{'─'*60}\n\n"
                
#                 # Add field descriptions
#                 field_meta_list = obj_meta.get('field_metadata', [])
#                 if field_meta_list:
#                     field_context += "**Field Descriptions:**\n\n"
#                     for field_meta in field_meta_list:
#                         field_context += f"• **{field_meta.get('field_name')}**\n"
#                         field_context += f"  - Type: {field_meta.get('datatype', 'Unknown')}\n"
#                         field_context += f"  - Description: {field_meta.get('description', 'N/A')}\n\n"
                
#                 field_context += f"**Available Fields:** {', '.join(obj_meta.get('fields', []))}\n\n"
            
#             field_context += "\n⚠️ CRITICAL RULES:\n"
#             field_context += "1. ONLY use fields explicitly listed above for each object\n"
#             field_context += "2. Read field descriptions carefully for filtering logic\n"
#             field_context += "3. Do NOT invent fields based on your training data\n\n"
            
#             resolved_prompt += field_context
            
#             total_fields = sum(len(obj_meta.get('field_metadata', [])) for obj_meta in all_objects.values())
#             logging.info(f"✅ Injected {total_fields} field descriptions across {len(all_objects)} objects")
            
#         elif relevant_fields and relevant_fields.get('field_metadata'):
#             # 🎯 SINGLE-OBJECT MODE (backward compatibility)
#             field_descriptions = []
            
#             for field_meta in relevant_fields.get('field_metadata', []):
#                 field_descriptions.append(f"""
# **{field_meta.get('field_name')}**:
# - Data Type: {field_meta.get('datatype', 'Unknown')}
# - Description: {field_meta.get('description', 'No description available')}
# """)
            
#             field_context = f"""

# ⚠️ CRITICAL SCHEMA INFORMATION FOR {selected_object} OBJECT:

# {' '.join(field_descriptions)}

# **Available Fields:** {', '.join(relevant_fields.get('fields', []))}

# ⚠️ ONLY use fields listed above.
# """
#             resolved_prompt += field_context
#             logging.info(f"✅ Injected {len(field_descriptions)} field descriptions for {selected_object}")
        
#         # SESSION CONTEXT
#         session_context = context.get("session_context", {}) if context else {}
#         shared_result_sets = context.get("shared_result_sets", {}) if context else {}
#         session_info = ""
        
#         if shared_result_sets:
#             for result_name, records in shared_result_sets.items():
#                 if records and isinstance(records, list):
#                     record_list = "\n".join([
#                         f"- {r.get('Name', r.get('Id', 'Unnamed'))} (ID: {r.get('Id')})" 
#                         for r in records[-50:]
#                     ])
#                     session_info += f"\n\nAVAILABLE {result_name.upper()} DATA:\n{record_list}"
        
#         if session_context:
#             created_records = session_context.get("created_records", {})
#             if created_records:
#                 for obj_type, records in created_records.items():
#                     if records:
#                         record_list = "\n".join([
#                             f"- {r.get('Name', 'Unnamed')} (ID: {r.get('Id')})" 
#                             for r in records[-50:]
#                         ])
#                         session_info += f"\n\nPREVIOUSLY CREATED {obj_type.upper()}:\n{record_list}"
        
#         # FETCH DEFAULT VALUES (dynamic based on all objects)
#         need_value_fields = []
#         default_value_map = {}
        
#         if all_objects:
#             objects_to_check = list(all_objects.keys())
#         else:
#             objects_to_check = [selected_object] if selected_object else []
        
#         if objects_to_check:
#             try:
#                 cm = get_chroma_manager()
#                 for obj in objects_to_check:
#                     fields_metadata = cm.get_need_value_fields(obj)
#                     for f in fields_metadata:
#                         fname = f.get('field_name')
#                         if fname:
#                             need_value_fields.append(fname)
#                             if f.get('defaultValue'):
#                                 default_value_map[fname] = f.get('defaultValue')
                
#                 need_value_fields = list(set(need_value_fields))
#                 logging.info(f"🔍 Need-value fields: {need_value_fields}")
#                 logging.info(f"🔍 Default values: {default_value_map}")
#             except Exception as e:
#                 logging.warning(f"⚠️ Failed to fetch need-value fields: {e}")
        
#         # BUILD ENHANCED QUERY
#         enhanced_user_query = user_query
#         defaults_instruction = ""
        
#         if need_value_fields:
#             import datetime
#             today = datetime.date.today()
            
#             def evaluate_default(val_str):
#                 try:
#                     val_lower = str(val_str).lower()
#                     if "today" in val_lower:
#                         parts = val_lower.split("+")
#                         days_add = 0
#                         if len(parts) > 1 and "day" in parts[1]:
#                             import re
#                             nums = re.findall(r'\d+', parts[1])
#                             if nums:
#                                 days_add = int(nums[0])
#                         return str(today + datetime.timedelta(days=days_add))
#                     return val_str
#                 except:
#                     return val_str
            
#             defaults_instruction = f"\n\n⚠️ MANDATORY DEFAULTS:\n"
#             defaults_instruction += f"Required fields: {', '.join(need_value_fields)}\n\n"
            
#             for field, raw_default in default_value_map.items():
#                 evaluated = evaluate_default(raw_default)
#                 defaults_instruction += f"- {field}: '{evaluated}' (if not specified)\n"
            
#             enhanced_user_query += defaults_instruction
        
#         if session_info:
#             enhanced_user_query = f"{user_query}{session_info}{defaults_instruction}"
        
#         enhanced_user_query += "\n\n⚠️ Use IDs from session context when user refers to 'this' or 'the' object."
        
#         logging.info(f"📋 Enhanced query length: {len(enhanced_user_query)} chars")
        
#         # CALL LLM
#         model = "gpt-4o"
#         provider = "OpenAI"
        
#         try:
#             raw_response = await asyncio.wait_for(
#                 call_llm(
#                     system_prompt=resolved_prompt,
#                     user_prompt=enhanced_user_query,
#                     default_model=model,
#                     default_provider=provider,
#                     default_temperature=0.0,
#                 ),
#                 timeout=30.0
#             )
#         except asyncio.TimeoutError:
#             logging.error("❌ LLM call timed out")
#             raise
        
#         logging.info(f"✅ LLM response received")
        
#         # Normalize response
#         if isinstance(raw_response, str):
#             content = raw_response.strip()
#         elif isinstance(raw_response, list):
#             content = " ".join(str(block) for block in raw_response).strip()
#         else:
#             content = str(raw_response).strip()
        
#         # Clean markdown
#         if content.startswith("```json"):
#             content = content[7:]
#         elif content.startswith("```"):
#             content = content[3:]
#         if content.endswith("```"):
#             content = content[:-3]
        
#         content = content.strip()
        
#         # Validate JSON
#         try:
#             parsed = json.loads(content)
#             logging.info(f"✅ Successfully parsed JSON with {len(parsed.get('calls', []))} tool calls")
#             return content
#         except json.JSONDecodeError as e:
#             logging.error(f"❌ Invalid JSON: {e}")
#             return json.dumps({
#                 "type": "error",
#                 "message": "Failed to generate valid JSON",
#                 "uiType": "ErrorMessage"
#             })
    
#     except Exception as e:
#         logging.error(f"❌ Error generating response: {e}", exc_info=True)
#         return json.dumps({
#             "type": "error",
#             "message": f"LLM error: {str(e)}",
#             "uiType": "ErrorMessage"
#         })


# async def generate_all_toolinput(
#     query: str,
#     context: Optional[Dict[str, Any]] = None
# ) -> Dict[str, Any]:
#     """
#     Main generate tool - fully dynamic multi-object support
#     """
#     try:
#         if not ensure_sf_connected():
#             return {
#                 "json_response": json.dumps({
#                     "type": "error",
#                     "message": "Salesforce connection failed",
#                     "uiType": "ErrorMessage"
#                 }),
#                 "context": context
#             }
        
#         logging.info(f"🚀 Generate tool called")
#         logging.info(f"   Query: {query[:100]}...")
        
#         # Extract context hints
#         context_object = None
#         if context and isinstance(context, dict):
#             task_directive = context.get("task_directive")
#             if task_directive:
#                 logging.info(f"📋 Task directive: {task_directive}")
#                 if "CampaignMember" in task_directive:
#                     context_object = "CampaignMember"
#                     query = task_directive
            
#             context_object = context.get("object") or context.get("Object") or context_object
        
#         # Find relevant objects and fields (multi-object aware)
#         selected_object, relevant_fields = find_relevant_objects_and_fields(
#             query,
#             context_object
#         )
        
#         if not selected_object:
#             return {
#                 "json_response": json.dumps({
#                     "type": "unsupported",
#                     "reason": "Could not identify relevant Salesforce object",
#                     "summary": "Unable to process this request"
#                 }),
#                 "context": context
#             }
        
#         # Generate response
#         response_content = await generate_structured_response(
#             query,
#             selected_object,
#             relevant_fields,
#             context
#         )
        
#         return {
#             "json_response": response_content
#         }
    
#     except Exception as e:
#         logging.error(f"❌ Error in generate_all_toolinput: {e}", exc_info=True)
#         return {
#             "json_response": json.dumps({
#                 "type": "error",
#                 "message": f"Generate tool error: {str(e)}",
#                 "uiType": "ErrorMessage"
#             }),
#             "context": context
#         }

from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import asyncio
import logging
import json
import time
import datetime
from dotenv import load_dotenv
import os

from Error.sf_error import SalesforceApiError
from client.sf_client import SalesforceClient
from core.helper import resolve_placeholders, call_llm, fetch_prompt_metadata
from openai import AsyncOpenAI
from chromadbutils import ChromaDBManager, chroma_client, schema_data, ensure_schema_initialized

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ChromadbLog.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)

# Global instances
sf_client = SalesforceClient("agent")
openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
_sf_connected = False
chroma_manager = None
_executor = None

def ensure_sf_connected():
    """Ensure Salesforce is connected"""
    global _sf_connected
    if not _sf_connected:
        _sf_connected = sf_client.connect()
    return _sf_connected


def get_executor():
    """Get or create shared thread pool executor"""
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=10)
    return _executor


def get_chroma_manager():
    """Get or create ChromaDB manager instance"""
    global chroma_manager
    if chroma_manager is None:
        chroma_manager = ChromaDBManager(chroma_client)
    return chroma_manager


@lru_cache(maxsize=1)
def get_object_relationships_cached():
    """Cache relationship mapping"""
    global schema_data
    if not schema_data:
        return {}
    
    relationships = {}
    
    if isinstance(schema_data, list):
        for obj_def in schema_data:
            obj_name = obj_def.get('object')
            if not obj_name:
                continue
            
            fields = obj_def.get('fields', [])
            related_objects = []
            
            for field in fields:
                field_name = field.get('apiname') or field.get('fieldapiname', '')
                if field_name.endswith('Id') and field_name != 'Id':
                    related_obj = field_name[:-2]
                    if related_obj:
                        related_objects.append(related_obj)
            
            if related_objects:
                relationships[obj_name] = related_objects
    
    return relationships


# Pre-defined common fields (optimization - no DB call needed)
COMMON_FIELDS_BY_OBJECT = {
    "campaign": ["Status", "StartDate", "EndDate", "Email_template__c", "Type"],
    "contact": ["Email", "FirstName", "LastName", "Phone", "AccountId"],
    "campaignmember": ["Status", "ContactId", "CampaignId"],
    "account": ["Type", "Industry", "BillingCity"],
    "lead": ["Status", "Email", "Company", "Phone"],
    "opportunity": ["StageName", "Amount", "CloseDate", "AccountId"],
    "case": ["Status", "Priority", "Subject", "AccountId", "ContactId"],
}


async def fetch_object_fields_optimized(cm, obj_name, user_query, executor):
    """
    Optimized field fetching that matches original code's accuracy
    Key: Uses same relaxed thresholds as original
    """
    try:
        loop = asyncio.get_running_loop()
        
        # ✅ Parallel execution but with original's parameters
        start_fetch = time.time()
        field_task = loop.run_in_executor(
            executor,
            lambda: cm.search_fields(obj_name, user_query, top_k=15)  # Same as original
        )
        name_task = loop.run_in_executor(
            executor,
            lambda: cm.search_fields(obj_name, "Name", top_k=1)
        )
        
        field_results, name_field_results = await asyncio.gather(field_task, name_task)
        
        fetch_time = time.time() - start_fetch
        logging.info(f"⏱️ Field fetch for {obj_name}: {fetch_time:.3f}s")
        
        if not field_results:
            return obj_name, None
        
        top_field = field_results[0]
        has_name = (len(name_field_results) > 0 and 
                   name_field_results[0]['field_name'] == 'Name')
        
        # Build selected fields
        selected_fields = ['Id']
        selected_field_metadata = []
        
        if has_name:
            selected_fields.append('Name')
        
        if top_field['field_name'] not in selected_fields:
            selected_fields.append(top_field['field_name'])
            selected_field_metadata.append(top_field)
        
        # ✅ SAME THRESHOLDS AS ORIGINAL (not aggressive)
        distance_threshold = 2.0  # Original's relaxed threshold
        max_fields = 15  # Original's max
        
        for field_result in field_results[1:]:
            if (field_result['distance'] < distance_threshold and
                    field_result['field_name'] not in selected_fields and
                    len(selected_fields) < max_fields):
                selected_fields.append(field_result['field_name'])
                selected_field_metadata.append(field_result)
        
        # ✅ Add common fields (same as original)
        obj_lower = obj_name.lower()
        if obj_lower in COMMON_FIELDS_BY_OBJECT:
            for common_field in COMMON_FIELDS_BY_OBJECT[obj_lower]:
                if common_field not in selected_fields:
                    selected_fields.append(common_field)
                    logging.debug(f"    💉 Added common field: {common_field}")
        
        return obj_name, {
            "object": obj_name,
            "fields": selected_fields,
            "field_metadata": selected_field_metadata,
            "filter_field": top_field['field_name'],
            "description": top_field['description'],
            "datatype": top_field['datatype']
        }
        
    except Exception as e:
        logging.error(f"❌ Error fetching fields for {obj_name}: {e}")
        return obj_name, None


async def find_relevant_objects_and_fields_async(user_query: str, context_object: str = None):
    """
    Optimized version - parallel execution with original's logic
    """
    global schema_data
    
    ensure_schema_initialized()
    import chromadbutils
    if hasattr(chromadbutils, 'schema_data') and chromadbutils.schema_data:
        schema_data = chromadbutils.schema_data

    logging.info(f"🔍 Processing query: '{user_query[:100]}...'")
    logging.debug(f"Context object hint: {context_object}")

    if not user_query or not user_query.strip():
        logging.warning("Empty user query provided")
        return None, None

    if not schema_data:
        logging.error("Schema data not initialized")
        return None, None

    try:
        cm = get_chroma_manager()
        executor = get_executor()
        start_time = time.time()
        
        # STEP 1: Semantic object search (async)
        loop = asyncio.get_running_loop()
        object_results = await loop.run_in_executor(
            executor,
            lambda: cm.search_objects(user_query, top_k=5)
        )
        
        if not object_results:
            logging.info("No relevant objects found")
            return None, None
        
        logging.info(f"🔍 Semantic search found {len(object_results)} objects:")
        for idx, obj in enumerate(object_results):
            logging.info(f"   {idx+1}. {obj['object_name']} (distance: {obj.get('distance', 'N/A'):.3f})")
        
        logging.info(f"⏱️ Object search: {time.time() - start_time:.2f}s")
        
        # STEP 2: Analyze query (same as original)
        query_lower = user_query.lower()
        
        action_verbs = {
            "find": ["find", "get", "fetch", "search", "list", "show", "select", "query"],
            "create": ["create", "new", "add", "make", "insert"],
            "update": ["update", "change", "modify", "edit", "set"],
            "delete": ["delete", "remove"],
            "assign": ["assign", "add to", "link", "connect", "attach"]
        }
        
        detected_actions = set()
        for action_type, verbs in action_verbs.items():
            if any(verb in query_lower for verb in verbs):
                detected_actions.add(action_type)
        
        logging.info(f"🎯 Detected actions: {list(detected_actions)}")
        
        # STEP 3: Determine primary object (same as original)
        RELEVANCE_THRESHOLD = 1.5
        relevant_objects = [
            obj for obj in object_results 
            if obj.get('distance', 999) < RELEVANCE_THRESHOLD
        ]
        
        logging.info(f"📦 Objects under threshold ({RELEVANCE_THRESHOLD}): {[obj['object_name'] for obj in relevant_objects]}")
        
        primary_object = None
        
        if context_object:
            primary_object = context_object
            logging.info(f"🎯 Using explicit context_object: {primary_object}")
        elif "find" in detected_actions or "search" in detected_actions:
            if relevant_objects:
                primary_object = relevant_objects[0]['object_name']
                logging.info(f"🎯 Primary object (for filtering): {primary_object}")
        else:
            primary_object = object_results[0]['object_name']
            logging.info(f"🎯 Primary object (semantic): {primary_object}")
        
        # STEP 4: Collect secondary objects (same as original)
        secondary_objects = []
        for obj in relevant_objects:
            obj_name = obj['object_name']
            if obj_name != primary_object:
                secondary_objects.append(obj_name)
        
        logging.info(f"📦 Secondary objects: {secondary_objects}")
        
        # STEP 5: Infer junction objects (same as original)
        objects_to_process = [primary_object] + secondary_objects
        objects_set = set(objects_to_process)
        
        object_relationships = get_object_relationships_cached()
        
        # 🧠 Junction Object Inference DISABLED per user request
        # for junction_obj, related_objs in object_relationships.items():
        #     if junction_obj in objects_set:
        #         continue
        #     
        #     connections = set(related_objs) & objects_set
        #     if len(connections) >= 2:
        #         logging.info(f"🧠 Inferred junction object: {junction_obj} (connects {connections})")
        #         objects_to_process.append(junction_obj)
        
        # ✅ OPTIMIZATION: Parallel field fetching (this is the speed gain)
        logging.info(f"🚀 Fetching {len(objects_to_process)} objects in parallel...")
        
        field_tasks = [
            fetch_object_fields_optimized(cm, obj_name, user_query, executor)
            for obj_name in objects_to_process
        ]
        
        field_results = await asyncio.gather(*field_tasks, return_exceptions=True)
        
        # Build metadata (same as original)
        all_objects_metadata = {}
        for result in field_results:
            if isinstance(result, Exception):
                logging.error(f"❌ Error: {result}")
                continue
            
            obj_name, obj_meta = result
            if obj_meta:
                all_objects_metadata[obj_name] = obj_meta
                logging.info(f"✅ {obj_name}: {len(obj_meta['fields'])} fields, {len(obj_meta['field_metadata'])} with metadata")
        
        if not all_objects_metadata:
            logging.warning("⚠️ No field metadata collected for any object")
            return primary_object, None
        
        # Return primary metadata (same as original)
        if primary_object in all_objects_metadata:
            primary_metadata = all_objects_metadata[primary_object]
        else:
            primary_object = list(all_objects_metadata.keys())[0]
            primary_metadata = all_objects_metadata[primary_object]
            logging.warning(f"⚠️ Using fallback primary object: {primary_object}")
        
        primary_metadata["all_objects"] = all_objects_metadata
        primary_metadata["context_object"] = context_object
        
        total_time = time.time() - start_time
        logging.info(f"✅ Total search: {total_time:.2f}s ({len(all_objects_metadata)} objects)")
        return primary_object, primary_metadata

    except Exception as e:
        logging.error(f"❌ Error: {e}", exc_info=True)
        return None, None


async def fetch_default_values_async(objects_to_check):
    """
    Parallel default fetching - optimization over original's sequential approach
    """
    try:
        cm = get_chroma_manager()
        loop = asyncio.get_running_loop()
        executor = get_executor()
        
        async def fetch_defaults(obj):
            try:
                logging.info(f"🔍 Fetching need_value fields for: {obj}")
                result = await loop.run_in_executor(
                    executor,
                    lambda: cm.get_need_value_fields(obj)
                )
                logging.info(f"   Found {len(result)} need_value fields for {obj}")
                return obj, result
            except Exception as e:
                logging.warning(f"⚠️ Failed to fetch need_value for {obj}: {e}")
                return obj, []
        
        results = await asyncio.gather(
            *[fetch_defaults(obj) for obj in objects_to_check],
            return_exceptions=True
        )
        
        need_value_fields = []
        default_value_map = {}
        
        for result in results:
            if isinstance(result, Exception):
                continue
            
            obj_name, fields_metadata = result
            
            for f in fields_metadata:
                fname = f.get('field_name')
                if fname:
                    need_value_fields.append(fname)
                    if f.get('defaultValue'):
                        default_value_map[fname] = f.get('defaultValue')
                    logging.info(f"   - {fname}: needValue={f.get('needValue')}, default={f.get('defaultValue')}")
        
        need_value_fields = list(set(need_value_fields))
        
        logging.info(f"🔍 Total need_value fields: {need_value_fields}")
        logging.info(f"🔍 Default values map: {default_value_map}")
        
        return need_value_fields, default_value_map
        
    except Exception as e:
        logging.error(f"❌ Failed to fetch defaults: {e}", exc_info=True)
        return [], {}


def filter_session_context(user_query: str, session_context: dict, shared_result_sets: dict) -> tuple:
    """
    🎯 DYNAMIC CONTEXT FILTER - Works for ANY Salesforce object
    
    Returns: (filtered_session_info: str, context_mode: str)
    """
    # Define language markers
    referential_keywords = [
        "this", "that", "the", "these", "those", "it", "them",
        "update", "modify", "change", "edit", "alter",
        "add to", "remove from", "delete from", "attach to",
        "link to", "connect to", "associate with"
    ]
    
    creation_keywords = [
        "new", "create", "make", "add a", "build", "set up",
        "fresh", "another", "generate"
    ]
    
    query_lower = user_query.lower()
    
    # Check for referential language
    has_referential = any(keyword in query_lower for keyword in referential_keywords)
    
    # Check for creation language
    has_creation = any(keyword in query_lower for keyword in creation_keywords)
    
    session_info = ""
    context_mode = "none"
    
    # Decision logic
    if has_referential and not (has_creation and "new" in query_lower):
        # User is REFERENCING existing data
        logging.info("🎯 CONTEXT FILTER: User is REFERENCING existing records, including session context")
        context_mode = "referencing"
        
        # Include shared result sets
        if shared_result_sets:
            for result_name, records in shared_result_sets.items():
                if records and isinstance(records, list):
                    record_list = "\n".join([
                        f"- {r.get('Name', r.get('Id', 'Unnamed'))} (ID: {r.get('Id')})"
                        for r in records[-10:]  # Show last 10 of each type
                    ])
                    session_info += f"\n\n✅ AVAILABLE {result_name.upper()} (from current session - user may be referencing):\n{record_list}"
        
        # Include created records
        if session_context:
            created_records = session_context.get("created_records", {})
            if created_records:
                for obj_type, records in created_records.items():
                    if records:
                        record_list = "\n".join([
                            f"- {r.get('Name', 'Unnamed')} (ID: {r.get('Id')})"
                            for r in records[-10:]
                        ])
                        session_info += f"\n\n✅ PREVIOUSLY CREATED {obj_type.upper()} (user may be referencing):\n{record_list}"
            
            # Add conversation history
            conversation_history = session_context.get("conversation_history", [])
            if conversation_history:
                last_action = conversation_history[-1] if conversation_history else {}
                if last_action:
                    session_info += f"\n\n📝 PREVIOUS REQUEST: {last_action.get('user_goal', '')}"
        
        session_info += "\n\n⚠️ USER IS REFERENCING EXISTING DATA: Use IDs from above when user says 'this/that/the'."
    
    elif has_creation or "new" in query_lower:
        # User is CREATING new records
        logging.info("🎯 CONTEXT FILTER: User is CREATING NEW records, omitting stale session context")
        context_mode = "creating"
        session_info = "\n\n⚠️ USER IS CREATING NEW RECORDS: Do NOT use session context unless user explicitly says 'add to this' or 'link to that'."
    
    else:
        # Ambiguous - include minimal context with warning
        logging.info("🎯 CONTEXT FILTER: Ambiguous intent, including minimal context with warning")
        context_mode = "ambiguous"
        
        if shared_result_sets or (session_context and session_context.get("created_records")):
            # Show counts only
            if shared_result_sets:
                for result_name, records in shared_result_sets.items():
                    if records and isinstance(records, list):
                        session_info += f"\n\n⚠️ AVAILABLE {result_name.upper()} (use only if user implies reference): {len(records)} record(s)"
            
            if session_context:
                created_records = session_context.get("created_records", {})
                if created_records:
                    for obj_type, records in created_records.items():
                        if records:
                            session_info += f"\n\n⚠️ PREVIOUSLY CREATED {obj_type.upper()} (use only if user implies reference): {len(records)} record(s)"
            
            session_info += "\n\n⚠️ AMBIGUOUS INTENT: Only use above context if user clearly references it. Default to creating NEW records."
        else:
            session_info = "\n\n⚠️ No previous session context available."
    
    logging.info(f"🎯 Context mode determined: {context_mode}")
    return session_info, context_mode


async def generate_structured_response(
    user_query: str,
    selected_object: Optional[str],
    relevant_fields: Optional[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None
) -> str:
    """
    Optimized response generation with dynamic context filtering
    """
    try:
        start_time = time.time()
        
        logging.info(f"🔍 generate_structured_response called")
        logging.info(f"   - Primary object: {selected_object}")
        logging.info(f"   - Query: {user_query[:100]}...")
        
        # ✅ OPTIMIZATION: Start prompt fetch early (parallel)
        prompt_task = asyncio.create_task(
            asyncio.to_thread(fetch_prompt_metadata, "generate_all_toolinput", "Marketing Agent")
        )
        
        # Build field context (same as original)
        field_context = ""
        all_objects = relevant_fields.get('all_objects', {}) if relevant_fields else {}
        
        if all_objects:
            logging.info(f"🎯 Multi-object mode: Injecting fields for {len(all_objects)} objects")
            
            field_context = "\n\n" + "="*60 + "\n"
            field_context += "⚠️ CRITICAL SCHEMA INFORMATION FOR MULTIPLE OBJECTS\n"
            field_context += "="*60 + "\n\n"
            field_context += "The following field information comes from the actual Salesforce schema.\n"
            field_context += "DO NOT use fields that are not listed here.\n\n"
            
            for obj_name, obj_meta in all_objects.items():
                field_context += f"\n{'─'*60}\n"
                field_context += f"📦 OBJECT: {obj_name}\n"
                field_context += f"{'─'*60}\n\n"
                
                field_meta_list = obj_meta.get('field_metadata', [])
                if field_meta_list:
                    field_context += "**Field Descriptions:**\n\n"
                    for field_meta in field_meta_list:
                        field_context += f"• **{field_meta.get('field_name')}**\n"
                        field_context += f"  - Type: {field_meta.get('datatype', 'Unknown')}\n"
                        field_context += f"  - Description: {field_meta.get('description', 'N/A')}\n\n"
                
                field_context += f"**Available Fields:** {', '.join(obj_meta.get('fields', []))}\n\n"
            
            field_context += "\n⚠️ CRITICAL RULES:\n"
            field_context += "1. ONLY use fields explicitly listed above for each object\n"
            field_context += "2. Read field descriptions carefully for filtering logic\n"
            field_context += "3. Do NOT invent fields based on your training data\n\n"
            
            total_fields = sum(len(obj_meta.get('field_metadata', [])) for obj_meta in all_objects.values())
            logging.info(f"✅ Will inject {total_fields} field descriptions across {len(all_objects)} objects")
            
        elif relevant_fields and relevant_fields.get('field_metadata'):
            field_descriptions = []
            
            for field_meta in relevant_fields.get('field_metadata', []):
                field_descriptions.append(f"""
**{field_meta.get('field_name')}**:
- Data Type: {field_meta.get('datatype', 'Unknown')}
- Description: {field_meta.get('description', 'No description available')}
""")
            
            field_context = f"""

⚠️ CRITICAL SCHEMA INFORMATION FOR {selected_object} OBJECT:

{' '.join(field_descriptions)}

**Available Fields:** {', '.join(relevant_fields.get('fields', []))}

⚠️ ONLY use fields listed above.
"""
            logging.info(f"✅ Will inject {len(field_descriptions)} field descriptions for {selected_object}")
        
        # 🎯 DYNAMIC SESSION CONTEXT FILTERING
        session_context = context.get("session_context", {}) if context else {}
        shared_result_sets_list = context.get("shared_result_sets", []) if context else []
        
        # Safety check: only access if list is non-empty and contains dicts
        if shared_result_sets_list and isinstance(shared_result_sets_list[-1], dict):
            shared_result_sets = shared_result_sets_list[-1]
        else:
            shared_result_sets = {}
        
        logging.info(f"🔍 Session context available: {bool(session_context)}")
        logging.info(f"🔍 Shared result sets available: {bool(shared_result_sets)}")
        
        # 🎯 Apply dynamic context filtering
        session_info, context_mode = filter_session_context(
            user_query,
            session_context,
            shared_result_sets
        )
        
        logging.info(f"🎯 Session context mode: {context_mode}")
        logging.info(f"🎯 Session info length: {len(session_info)} chars")
        
        # ✅ OPTIMIZATION: Fetch defaults in parallel with prompt
        objects_to_check = list(all_objects.keys()) if all_objects else ([selected_object] if selected_object else [])
        
        defaults_task = asyncio.create_task(fetch_default_values_async(objects_to_check))
        
        # Wait for prompt
        prompt_metadata = await prompt_task
        resolved_prompt = resolve_placeholders(
            prompt=prompt_metadata["prompt"],
            configs=prompt_metadata["configs"],
            state=context
        )
        resolved_prompt += field_context
        
        logging.info(f"⏱️ Prompt prep: {time.time() - start_time:.2f}s")
        
        # 🧠 CONDITIONAL EXECUTION RULES
        conditional_rules = """
\n⚠️ CONDITIONAL EXECUTION RULES:
If your plan involves creating or upserting a record (like upsert_salesforce_records) that EITHER:
a) Is being searched for in a previous step (e.g., via run_dynamic_soql).
b) Is ALREADY present in the "shared_result_sets" context.

YOU MUST:
1. Use the `record_id` placeholder from the result (e.g., `{{search_result.Id}}`).
2. **IMPORTANT - skip_if_exists**: ALWAYS include `"skip_if_exists": "[key]"` if the search result pertains to the **EXACT SAME object type** you are about to create/upsert.
   - Example: If you searched for "CampaignMemberStatus" and stored it as "draft_status", YOU MUST include `"skip_if_exists": "draft_status"` in the CampaignMemberStatus upsert call.
   - Example (FAIL): NEVER use `skip_if_exists` on "CampaignMember" because you found a "Contact". They are different objects.
   - Replace "[key]" with the actual `store_as` name from the search step.

"""
        resolved_prompt += conditional_rules
        
        # Build base query
        enhanced_user_query = user_query
        
        # Wait for defaults
        need_value_fields, default_value_map = await defaults_task
        
        # Build defaults instruction (same as original)
        if need_value_fields:
            today = datetime.date.today()
            
            def evaluate_default(val_str):
                try:
                    val_lower = str(val_str).lower()
                    if "today" in val_lower:
                        parts = val_lower.split("+")
                        days_add = 0
                        if len(parts) > 1 and "day" in parts[1]:
                            import re
                            nums = re.findall(r'\d+', parts[1])
                            if nums:
                                days_add = int(nums[0])
                        return str(today + datetime.timedelta(days=days_add))
                    return val_str
                except:
                    return val_str
            
            defaults_instruction = f"\n\n⚠️ MANDATORY REQUIRED FIELDS (MUST INCLUDE):\n"
            defaults_instruction += f"Fields: {', '.join(need_value_fields)}\n\n"
            defaults_instruction += "**Use these default values:**\n"
            
            for field, raw_default in default_value_map.items():
                evaluated = evaluate_default(raw_default)
                defaults_instruction += f"- {field}: '{evaluated}'\n"
            
            defaults_instruction += "\n⚠️ CRITICAL: Include ALL required fields above in create/propose_action calls.\n"
            
            enhanced_user_query += defaults_instruction
        
        # ✅ Add filtered session info
        if session_info:
            enhanced_user_query += session_info
        
        logging.info(f"📋 Enhanced query length: {len(enhanced_user_query)} chars")
        
        # Call LLM (same as original)
        try:
            raw_response = await asyncio.wait_for(
                call_llm(
                    system_prompt=resolved_prompt,
                    user_prompt=enhanced_user_query,
                    default_model="gpt-4o",
                    default_provider="OpenAI",
                    default_temperature=0.0,
                ),
                timeout=30.0
            )
        except asyncio.TimeoutError:
            logging.error("❌ LLM call timed out")
            raise
        
        logging.info(f"⏱️ LLM call: {time.time() - start_time:.2f}s")
        
        # Parse response (same as original)
        if isinstance(raw_response, str):
            content = raw_response.strip()
        elif isinstance(raw_response, list):
            content = " ".join(str(block) for block in raw_response).strip()
        else:
            content = str(raw_response).strip()
        
        if content.startswith("```json"):
            content = content[7:]
        elif content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
        
        content = content.strip()
        
        try:
            parsed = json.loads(content)
            logging.info(f"✅ Successfully parsed JSON with {len(parsed.get('calls', []))} tool calls")
            logging.info(f"⏱️ TOTAL: {time.time() - start_time:.2f}s")
            return content
        except json.JSONDecodeError as e:
            logging.error(f"❌ Invalid JSON: {e}")
            return json.dumps({
                "type": "error",
                "message": "Failed to generate valid JSON",
                "uiType": "ErrorMessage"
            })
    
    except Exception as e:
        logging.error(f"❌ Error generating response: {e}", exc_info=True)
        return json.dumps({
            "type": "error",
            "message": f"LLM error: {str(e)}",
            "uiType": "ErrorMessage"
        })


async def generate_all_toolinput(
    query: str,
    context: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Optimized main generate tool with dynamic context filtering
    """
    # 🔴 FORCE LOGGING SETUP
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("ChromadbLog.log", mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    try:
        # if not ensure_sf_connected():
        #     return {
        #         "json_response": json.dumps({
        #             "type": "error",
        #             "message": "Salesforce connection failed",
        #             "uiType": "ErrorMessage"
        #         })
        #     }
        
        start_time = time.time()
        logging.info(f"🚀 Generate tool called")
        logging.info(f"   Query: {query[:100]}...")
        
        # Extract context (same as original)
        context_object = None
        if context and isinstance(context, dict):
            task_directive = context.get("task_directive")
            if task_directive:
                logging.info(f"📋 Task directive: {task_directive}")
                if "CampaignMember" in task_directive:
                    context_object = "CampaignMember"
                    query = task_directive
            
            context_object = context.get("object") or context.get("Object") or context_object
        
        # Find objects (optimized with async)
        selected_object, relevant_fields = await find_relevant_objects_and_fields_async(
            query,
            context_object
        )
        
        if not selected_object:
            return {
                "json_response": json.dumps({
                    "type": "unsupported",
                    "reason": "Could not identify relevant Salesforce object",
                    "summary": "Unable to process this request"
                })
            }
        
        logging.info(f"⏱️ Object/field search: {time.time() - start_time:.2f}s")
        
        # Generate response (optimized with parallel fetches + dynamic context filtering)
        response_content = await generate_structured_response(
            query,
            selected_object,
            relevant_fields,
            context
        )
        
        logging.info(f"⏱️ TOTAL EXECUTION TIME: {time.time() - start_time:.2f}s")
        
        return {
            "json_response": response_content
        }
    
    except Exception as e:
        logging.error(f"❌ Error in generate_all_toolinput: {e}", exc_info=True)
        return {
            "json_response": json.dumps({
                "type": "error",
                "message": f"Generate tool error: {str(e)}",
                "uiType": "ErrorMessage"
            })
        }