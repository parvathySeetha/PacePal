from core.helper import get_member_dependency, fetch_prompt_metadata, resolve_placeholders, call_llm
from langchain_core.messages import AIMessage
import logging
from agents.marketing.state import MarketingState


async def marketing_orchestrator(state: MarketingState) -> MarketingState:
    """
    Simplified Marketing orchestrator - all logic is now in Marketing Agent Orchestrator prompt template.
    This node just:
    3. Calls LLM
    4. Routes based on response
    """
    logging.info("🎯 Orchestrator analyzing workflow...")
     
    # Iteration guard
    state["iteration_count"] = state.get("iteration_count", 0) + 1
    if state["iteration_count"] >= state.get("max_iterations", 15):
        logging.warning("Max iterations reached, completing workflow")
        state["next_action"] = "complete"
        state["error"] = "Maximum iterations reached"
        return state

    # Get registry for services_info
    parent_member = state.get("parent_member", "Marketing Agent")
    registry = get_member_dependency(parent_member=parent_member)
     
    # Build services_info string
    services_info = "\n".join(
        f"- {name}: {meta.get('description', 'No description')}"
        for name, meta in registry.items()
    )
    
    # Build progress summary
    progress_summary = _build_progress_summary(state)
    
    logging.info(f"🔍 [Orchestrator] Progress Summary:\n{progress_summary}")

    # Store dynamic values in state for placeholder resolution
    state["services_info"] = services_info
    state["progress_summary"] = progress_summary
    
    # Valid actions for validation (casual_chat will be handled separately)
    logging.info(f"Valid actions: {list(registry.keys())}")
    valid_actions = list(registry.keys()) + ["complete","EngagementWorkflow", "Email Builder Agent", "EmailBuilderAgent"]
    state["valid_actions"] = valid_actions
    
    # Fetch prompt from Salesforce
    prompt_meta = fetch_prompt_metadata("marketing_orchestrator","Marketing Agent")
    if not prompt_meta:
        logging.error("❌ Marketing Agent Orchestrator Prompt not found in Salesforce!")
        state["next_action"] = "complete"
        state["error"] = "Orchestrator prompt template not found"
        return state

    # Resolve placeholders
    resolved_prompt = resolve_placeholders(
        prompt=prompt_meta["prompt"],
        configs=prompt_meta["configs"],
        state=state
    )
    
    # Build conversation history
    messages = state.get("messages", [])
    history_lines = []
    # Take last 5 messages to preserve context without blowing up tokens
    for msg in messages[-5:]:
        role = "User" if msg.type == "human" else "Assistant"
        content = str(msg.content)
        # Truncate very long messages
        if len(content) > 200:
            content = content[:200] + "..."
        history_lines.append(f"{role}: {content}")
    
    conversation_history = "\n".join(history_lines) if history_lines else "No history yet."
    logging.info(f"user_goal in satte is :\n{state['user_goal']}")
    # Build user prompt
    user_prompt = f"""User Goal: {state['user_goal']}

Progress So Far:
{progress_summary}

Recent Conversation History:
{conversation_history}

Based on the User Goal and Progress Summary above:
- If the goal is ALREADY realized by the completed operations, respond with 'complete'
- If there is NEW work to be done, choose the next agent
- **PRIORITY**: If the user asks to "track engagement", "check clicks", "find interested members", or "analyze links", you MUST route to 'EngagementWorkflow'.
- **DEPENDENCY**: If the goal involves sending email to a list/campaign, you MUST route to 'Salesforce MCP' first to fetch contacts, unless 'contacts' or 'CampaignMember' results are already listed in the Progress Summary. Do NOT route to 'Brevo MCP' until contacts are available.
- **REVISION**: If the user wants to EDIT, REFINE, CHANGE, or BOLD/FORMAT a previously generated email (or "make it bold", "change the subject"), you MUST route to 'Email Builder Agent'.
- **COMPLETION**: If the Progress Summary shows successful 'upsert' operations for Campaign and CampaignMember (or contacts), and the goal was to create/add members, YOU MUST respond with 'complete'.
- Do NOT repeat successful operations

What should we do next? Respond with ONLY one of: Salesforce MCP, Brevo MCP, Linkly MCP , EngagementWorkflow, Email Builder Agent, complete, casual_chat:{{message}}"""

    try:
        # Call LLM
        raw_response = await call_llm(
            system_prompt=resolved_prompt,
            user_prompt=user_prompt,
            default_model=prompt_meta["model"],
            default_provider=prompt_meta["provider"],
            default_temperature=0.0,
        )
        
        normalized = raw_response.strip()
        logging.info(f"Orchestrator decision (raw): {raw_response}")
        logging.info(f"Orchestrator decision (normalized): {normalized}")

        # Handle casual chat response
        if normalized.startswith("casual_chat:"):
            logging.info("💬 Casual chat detected - generating witty response")
            user_message = normalized.replace("casual_chat:", "").strip()
            
            # Generate witty, contextual response using LLM
            casual_prompt = f"""The user said: "{user_message}"

Generate a fun, witty, clever response that:
1. Directly replies to their message in a playful way
2. Briefly mentions you're a Marketing Agent (1-2 sentences max)
3. Hints at your capabilities (Salesforce, Brevo, Linkly)

Keep it conversational, friendly, and engaging. No formal lists or bullet points."""

            try:
                witty_response = await call_llm(
                    system_prompt="You are a friendly, witty Marketing Agent assistant.",
                    user_prompt=casual_prompt,
                    default_model=prompt_meta["model"],
                    default_provider=prompt_meta["provider"],
                    default_temperature=0.7,  # Higher temperature for creativity
                )
                
                state["next_action"] = "complete"
                state["final_response"] = witty_response.strip()
                return state
                
            except Exception as e:
                logging.error(f"Failed to generate casual response: {e}")
                # Fallback to simple response
                state["next_action"] = "complete"
                state["final_response"] = f"Hey there! 👋 I'm your Marketing Agent, ready to help with Salesforce campaigns, Brevo emails, and Linkly tracking links. What can I do for you today?"
                return state

        # Validate response
        if normalized not in valid_actions:
            logging.warning(f"Invalid routing decision: {raw_response}, defaulting to complete")
            normalized = "complete"

        state["next_action"] = normalized
        state["current_agent"] = "orchestrator"

        # state.setdefault("messages", [])
        # state["messages"].append(
        #     AIMessage(content=f"Orchestrator decision: Route to {normalized}")
        # )

        logging.info(f"✅ Routing decision: {normalized}")

    except Exception as e:
        logging.error(f"Orchestrator error: {e}", exc_info=True)
        state["error"] = f"Orchestrator failed: {str(e)}"
        state["next_action"] = "complete"

    return state



def _build_progress_summary(state: MarketingState) -> str:
    """
    Build a DETAILED, DYNAMIC summary of all MCP executions with ENTITY NAMES.
    """
    import json
    import re
    
    summary_parts = []
    
    # Show pending work FIRST
    task_directive = state.get("task_directive")
    pending_updates = state.get("pending_updates")
    
    if task_directive or pending_updates:
        pending_section = "⚠️  PENDING WORK:\n"
        
        if task_directive:
            pending_section += f"  🎯 Directive: {task_directive}\n"
        
        if pending_updates:
            operation = pending_updates.get("operation", "unknown")
            reason = pending_updates.get("reason", "")
            pending_section += f"  📌 Operation: {operation}\n"
            if reason:
                pending_section += f"  📝 Reason: {reason}\n"
        
        summary_parts.append(pending_section)
    
    # Check for Generated Email Content
    generated_email = state.get("generated_email_content")
    if generated_email:
        subject = generated_email.get("subject", "No subject")
        summary_parts.append(f"✅ EMAIL CONTENT GENERATED:\n  Subject: {subject}\n  (Content available in state)")

    # 🆕 Extract entity names from shared_result_sets for context (FULL SESSION HISTORY)
    shared_results_list = state.get("shared_result_sets", []) or []
    # 🔄 Flatten history for comprehensive lookup
    shared_results = {}
    for rs in shared_results_list:
        if rs and isinstance(rs, dict):
            shared_results.update(rs)
    entity_context = {}
    
    if shared_results:
        for key, records in shared_results.items():
            if key == "_metadata":
                continue
            if isinstance(records, list) and records:
                # Extract names from records
                names = []
                for rec in records[:5]:  # Show first 5
                    if isinstance(rec, dict):
                        name = rec.get("Name") or rec.get("Id", "Unknown")
                        names.append(name)
                
                entity_context[key] = {
                    "count": len(records),
                    "names": names
                }

    # MCP results summary
    mcp_results = state.get("mcp_results", {})
    if not mcp_results:
        if summary_parts:
            return "\n\n".join(summary_parts)
        return "ℹ️ No MCPs have been called yet."
    
    for service_name, data in mcp_results.items():
        if not data:
            continue
            
        exec_summary = data.get("execution_summary", {})
        tool_results = data.get("tool_results", [])
        
        if exec_summary:
            total_calls = exec_summary.get("total_calls", 0)
            successful = exec_summary.get("successful_calls", 0)
            skipped = exec_summary.get("skipped_calls", 0)
            failed = exec_summary.get("failed_calls", 0)
            
            # Extract operations details with ENTITY NAMES
            operations_detail = []
            for result in tool_results[-10:]:  # Last 10 operations
                tool_name = result.get("tool_name", "unknown")
                status = result.get("status", "unknown")
                
                # Try to extract summary from response
                response_obj = result.get("response")
                tool_output_text = ""
                entity_info = ""
                
                if status == "skipped":
                    tool_output_text = result.get("reason", "Already exists")
                
                if response_obj and hasattr(response_obj, 'content'):
                    try:
                        texts = []
                        for item in response_obj.content:
                            if hasattr(item, 'text'):
                                texts.append(item.text)
                        if texts:
                            tool_output_text = " | ".join(texts)
                            
                            # 🆕 Try to extract entity names from JSON response
                            try:
                                # Look for JSON in the text
                                json_match = re.search(r'\{.*\}', tool_output_text, re.DOTALL)
                                if json_match:
                                    json_data = json.loads(json_match.group())
                                    
                                    # Extract Campaign name
                                    if "results" in json_data and isinstance(json_data["results"], list):
                                        for res in json_data["results"]:
                                            if res.get("success") and "record_id" in res:
                                                entity_info = f" [ID: {res['record_id']}]"
                                    
                                    # Extract record count from SOQL
                                    if "records" in json_data and isinstance(json_data["records"], list):
                                        count = len(json_data["records"])
                                        if count > 0 and json_data["records"][0].get("Name"):
                                            first_name = json_data["records"][0]["Name"]
                                            entity_info = f" [Found {count} records, first: '{first_name}']"
                                        else:
                                            entity_info = f" [Found {count} records]"
                            except:
                                pass
                            
                            if len(tool_output_text) > 500:
                                tool_output_text = tool_output_text[:497] + "..."
                    except Exception:
                        pass
                
                if tool_output_text:
                    op_desc = f"{tool_name}{entity_info} -> {tool_output_text}"
                else:
                    # Fallback: show request arguments with entity extraction
                    request = result.get("request", {})
                    details = []
                    
                    # 🆕 Extract Campaign/Contact names from request
                    if "records" in request and isinstance(request["records"], list):
                        for rec in request["records"][:1]:  # Show first record
                            if isinstance(rec, dict) and "fields" in rec:
                                fields = rec["fields"]
                                if "Name" in fields:
                                    entity_info = f" [Name: '{fields['Name']}']"
                    
                    for k, v in request.items():
                        if isinstance(v, dict):
                            flat_v = ", ".join(f"{sub_k}={sub_v}" for sub_k, sub_v in v.items())
                            details.append(f"{k}: {{{flat_v}}}")
                        else:
                            details.append(f"{k}={v}")
                    
                    args_str = ", ".join(details)
                    op_desc = f"{tool_name}{entity_info} ({args_str})"

                operations_detail.append(f"{op_desc} ({status})")
            
            ops_str = "\n  - ".join(operations_detail) if operations_detail else "No specific operations"
            
            # 🆕 Add entity context summary
            context_str = ""
            if entity_context:
                context_lines = []
                for entity_type, info in entity_context.items():
                    count = info["count"]
                    names = info["names"]
                    if names:
                        names_str = ", ".join(f"'{n}'" for n in names[:3])
                        if count > 3:
                            names_str += f" (+ {count - 3} more)"
                        context_lines.append(f"    • {entity_type}: {count} records ({names_str})")
                    else:
                        context_lines.append(f"    • {entity_type}: {count} records")
                
                if context_lines:
                    context_str = "\n  Context:\n" + "\n".join(context_lines)
            
            summary_parts.append(
                f"✅ {service_name.upper()} COMPLETED:\n"
                f"  Stats: {total_calls} calls ({successful} successful, {skipped} skipped, {failed} failed){context_str}\n"
                f"  Operations:\n  - {ops_str}"
            )
        else:
            summary_parts.append(f"⚠️ {service_name}: Called but no detailed summary available")

    if not summary_parts:
        return "ℹ️ No operations recorded yet."
    
    return "\n\n".join(summary_parts)

