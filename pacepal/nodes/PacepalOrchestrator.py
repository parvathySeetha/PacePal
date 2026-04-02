from pacepal.state import PacepalState
from shared.config import DEPLOYMENT_MODE
import logging
from core.helper import get_member_dependency, fetch_prompt_metadata, resolve_placeholders, call_llm

logger = logging.getLogger(__name__)

async def pacepal_orchestrator(state: PacepalState) -> PacepalState:
    """
    Top-level orchestrator that routes to appropriate agents.
    Uses LLM to intelligently select agent based on user goal and agent descriptions.
    """
    logger.info("🧠 [PacePal Orchestrator] Analyzing request...")
    logger.info(f"   Session ID: {state.get('session_id')}, Record ID: {state.get('record_id')}")
    logger.info(f"   Deployment mode: {DEPLOYMENT_MODE}")
    logger.info(f"   User goal: {state.get('user_goal', 'No goal')[:100]}...")
    
    # Get all available agents for PacePal
    parent_member = "Pacepal Agent"
    registry = get_member_dependency(parent_member=parent_member)
    
    if not registry:
        logger.warning("⚠️ No agents found in registry, defaulting to Marketing Agent")
        state["selected_agent"] = "Marketing Agent"
        return state
    
    # Build agent info string with descriptions
    agent_info = []
    for name, meta in registry.items():
        description = meta.get('description', 'No description')
        intent = meta.get('intent', '')
        agent_info.append(f"**{name}**\n  Description: {description}\n  Intent: {intent}")
    
    agents_description = "\n\n".join(agent_info)
    agent_names = list(registry.keys())
    
    logger.info(f"📋 Found {len(agent_names)} available agents: {agent_names}")
    
    # Fetch routing prompt from Salesforce
    try:
        prompt_meta = fetch_prompt_metadata("pacepal_orchestrator", "Pacepal Agent")
        
        if not prompt_meta:
            logger.warning("⚠️ PacePal orchestrator prompt not found, using fallback")
            # Fallback to simple routing
            state["selected_agent"] = agent_names[0] if agent_names else "Marketing Agent"
            return state
        
        # Prepare state for placeholder resolution
        state["agents_description"] = agents_description
        state["agent_names"] = ", ".join(agent_names)
        
        # Resolve placeholders in prompt
        resolved_prompt = resolve_placeholders(
            prompt=prompt_meta["prompt"],
            configs=prompt_meta["configs"],
            state=state
        )
        

        # Build conversation history
        messages = state.get("messages", [])
        history_lines = []
        for msg in messages[-5:]:
            role = "User" if msg.type == "human" else "Assistant"
            content = str(msg.content)
            if len(content) > 200:
                content = content[:200] + "..."
            history_lines.append(f"{role}: {content}")
        
        history_text = "\n".join(history_lines) if history_lines else "No history."

        # Build user prompt with history
        user_prompt = f"""User Request: {state.get('user_goal', '')}

Conversation History:
{history_text}

Task: Choose the best agent to handle the User Request based on the history."""
        
        # Call LLM for routing decision
        raw_response = await call_llm(
            system_prompt=resolved_prompt,
            user_prompt=user_prompt,
            default_model=prompt_meta["model"],
            default_provider=prompt_meta["provider"],
            default_temperature=0.0,
        )
        
        decision = raw_response.strip()
        
        # Validate decision
        if decision not in agent_names:
            logger.warning(f"⚠️ LLM returned invalid agent '{decision}', using first available")
            decision = agent_names[0]
        
        logger.info(f"👉 [PacePal Orchestrator] Routing to: {decision}")
        state["selected_agent"] = decision
        
    except Exception as e:
        logger.error(f"❌ Error in intelligent routing: {e}", exc_info=True)
        # Fallback to first agent
        decision = agent_names[0] if agent_names else "Marketing Agent"
        logger.warning(f"⚠️ Falling back to: {decision}")
        state["selected_agent"] = decision
    
    return state
