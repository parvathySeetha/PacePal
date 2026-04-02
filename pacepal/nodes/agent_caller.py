from pacepal.state import PacepalState
from pacepal.agent_client import AgentClient
from shared.models import AgentRequest, AgentResponse
import logging
from langgraph.types import Interrupt, interrupt
from core.helper import get_member_dependency

logger = logging.getLogger(__name__)

# Initialize agent client for remote calls
agent_client = AgentClient()

async def call_dynamic_agent(state: PacepalState) -> PacepalState:
    """
    Execute a selected agent dynamically based on its registry endpoint.
    """
    selected_agent = state.get("selected_agent")
    if not selected_agent:
        logger.error("❌ [PacePal] No agent selected in state")
        state["error"] = "No agent selected"
        return state

    # Get registry to find the endpoint
    registry = get_member_dependency("Pacepal Agent")
    agent_meta = registry.get(selected_agent)

    if not agent_meta:
        logger.error(f"❌ [PacePal] Agent '{selected_agent}' not found in registry")
        state["error"] = f"Agent '{selected_agent}' not found in registry"
        return state

    execution_endpoint = agent_meta.get("executionEndpoint")
    if not execution_endpoint:
        logger.error(f"❌ [PacePal] No executionEndpoint found for agent '{selected_agent}'")
        state["error"] = f"No executionEndpoint found for agent '{selected_agent}'"
        return state

    # executionEndpoint might be a JSON string or a direct URL
    agent_url = execution_endpoint
    if isinstance(execution_endpoint, str) and execution_endpoint.startswith("["):
        try:
            import json
            parsed = json.loads(execution_endpoint)
            if isinstance(parsed, list) and len(parsed) > 0:
                agent_url = parsed[0]
        except:
            pass

    logger.info(f"🌐 [PacePal] Calling dynamic agent '{selected_agent}' at {agent_url}")

    # Prepare request
    # Serialize messages
    messages = state.get("messages", [])
    serialized_messages = []
    for msg in messages:
        if hasattr(msg, "model_dump"):
            serialized_messages.append(msg.model_dump())
        elif hasattr(msg, "dict"):
            serialized_messages.append(msg.dict())
        else:
            serialized_messages.append(dict(msg))

    resume_value = state.get("resume_data")

    logger.info(f"🌐 [PacePal] Sending request to '{selected_agent}'. Session ID: {state.get('session_id')}, Record ID: {state.get('record_id')}")
    
    request = AgentRequest(
        user_goal=state.get("user_goal", ""),
        context={k: v for k, v in state.items() if k not in ["messages", "resume_data"]},
        session_id=state.get("session_id", "default"),
        messages=serialized_messages,
        resume=resume_value
    )

    try:
        # Call remote agent
        response: AgentResponse = await agent_client.call_agent(selected_agent, request, agent_url=agent_url)
    except Exception as e:
        logger.exception(f"❌ [PacePal] Dynamic agent connection error ({selected_agent}): {e}")
        state["error"] = str(e)
        state["final_response"] = f"Failed to reach agent '{selected_agent}': {str(e)}"
        return state

    # Handle remote interrupt
    # CRITICAL: Do NOT wrap interrupt() in a try-except Exception block
    if response.status == "interrupted":
        logger.info(f"⏸️ [PacePal] Agent '{selected_agent}' interrupted (user approval needed)")
        
        # Pause and wait for user input (raises BaseException)
        user_approval_data = interrupt([Interrupt(value=response.final_response)])
        
        # Forward approval to remote agent
        logger.info(f"▶️ [PacePal] Resumed with approval data: {user_approval_data}")
        
        resume_request = AgentRequest(
            user_goal=state["user_goal"],
            context=state,
            session_id=state.get("session_id", "default"),
            messages=[],
            resume=user_approval_data
        )
        
        try:
            logger.info(f"🌐 [PacePal] Sending RESUME request to agent '{selected_agent}'...")
            response = await agent_client.call_agent(selected_agent, resume_request, agent_url=agent_url)
        except Exception as e:
            logger.exception(f"❌ [PacePal] Dynamic agent resume error ({selected_agent}): {e}")
            state["error"] = str(e)
            state["final_response"] = f"Failed to resume agent '{selected_agent}': {str(e)}"
            return state

    # Merge response
    state.update(response.result)
    state["final_response"] = response.final_response
    state["error"] = response.error
    
    if response.created_records:
        state["created_records"] = response.created_records
    if response.generated_email_content:
        state["generated_email_content"] = response.generated_email_content

    logger.info(f"✅ [PacePal] Agent '{selected_agent}' completed")

    return state
