from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel
# from shared.models import AgentRequest, AgentResponse
import asyncio
import os
import sys
from pathlib import Path

# Add ioagent directory to sys.path for local imports
current_file = Path(__file__).resolve()
ioagent_dir = current_file.parent

if str(ioagent_dir) not in sys.path:
    sys.path.insert(0, str(ioagent_dir))

# Add project root to sys.path to allow importing from mcp_module (two levels up)
project_root = ioagent_dir.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

import json
import logging

from io_agent2 import io_agent, IOState
from shared.models import AgentRequest, AgentResponse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("io_agent.log"),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)

app = FastAPI()

# --- WebSocket Manager ---
import connection_manager
from connection_manager import manager

# --- WebSocket Manager ---
# --- WebSocket Manager ---
# Moved to connection_manager.py

# --- Prompts ---
from core.helper import preload_prompts_for_consumer, _prompt_cache
GLOBAL_PROMPTS = {}

@app.on_event("startup")
async def startup_event():
    connection_manager.main_event_loop = asyncio.get_running_loop()
@app.on_event("startup")
async def startup_event():
    connection_manager.main_event_loop = asyncio.get_running_loop()
    logger.info(f"Main event loop captured: {connection_manager.main_event_loop}")
    
    # Fetch Prompts via Mongo Preload
    global GLOBAL_PROMPTS
    logger.info("📦 Preloading prompts for IO Agent from MongoDB...")
    success = preload_prompts_for_consumer("IO Agent")
    
    if success:
        # Convert _prompt_cache to the list-based format expected by nodes.py
        # Format: [template_text, llm_provider, llm_model, configs_map]
        GLOBAL_PROMPTS = {
            name: [
                val['prompt'], 
                val['provider'], 
                val['model'], 
                {c.name: c.default_value for c in val['configs']}
            ]
            for name, val in _prompt_cache.items()
        }
        logger.info(f"Prompts preloaded successfully: {len(GLOBAL_PROMPTS)} templates cached.")
    else:
        logger.error("Failed to preload prompts for IO Agent")

# --- Session Persistence (File-based) ---
SESSION_DIR = ioagent_dir / "sessions"
SESSION_DIR.mkdir(exist_ok=True)

def _get_session_path(session_id: str) -> Path:
    return SESSION_DIR / f"{session_id}.json"

def _save_session(session_id: str, state: dict):
    if not session_id:
        return
    try:
        # Convert state dict to IOState and then to a JSON-safe dict
        # This ensuring everything is serializable (dates, pydantic sub-models, etc)
        json_safe_state = IOState(**state).model_dump(mode='json')
        with open(_get_session_path(session_id), "w") as f:
            json.dump(json_safe_state, f)
        logger.info(f"💾 Session {session_id} saved to disk.")
    except Exception as e:
        logger.error(f"Failed to save session {session_id}: {e}")

def _load_session(session_id: str) -> dict:
    if not session_id:
        return {}
    path = _get_session_path(session_id)
    if path.exists():
        try:
            with open(path, "r") as f:
                data = json.load(f)
            logger.info(f"📂 Session {session_id} loaded from disk.")
            return data
        except Exception as e:
            logger.error(f"Failed to load session {session_id}: {e}")
    return {}

# --- WebSocket Endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    #current_case_id = "500f6000007HSbmAAG" # Default fallback
    current_case_id = None
    
    # Session state to persist across multiple turns in a single websocket connection
    session_state = IOState()
    
    try:
        while True:
            data = await websocket.receive_text()
            
            try:
                message_data = json.loads(data)
                logger.info(f"📥 Received JSON: {json.dumps(message_data, indent=2)}")
                
                msg_type = message_data.get("type", "message")
                
                # Check for recordId in any message
                rec_id = message_data.get("recordId")
                if rec_id and rec_id.startswith("500") and rec_id != current_case_id:
                    current_case_id = rec_id
                    logger.info(f"🔄 Updated Case ID to: {current_case_id}")

                if msg_type == "connection_init":
                    if rec_id and rec_id.startswith("500"):
                        current_case_id = rec_id
                        logger.info(f"Set Case ID to: {current_case_id}")
                        await manager.send_personal_message(json.dumps({
                            "type": "status", 
                            "message": f"Connected to Case {current_case_id}"
                        }), websocket)
                    continue

                user_message = message_data.get("message", "")
            except json.JSONDecodeError:
                logger.info(f"📥 Received Text: {data}")
                user_message = data

            # Send acknowledgement
            await manager.send_personal_message(json.dumps({
                "type": "status", 
                "message": "Agent processing..."
            }), websocket)

            # Prepare Initial State (Merge with session state if existing)
            if session_state.io_markdown or session_state.case_id:
                logger.info("Restoring IO Agent state from session...")
                # Map current state values
                initial_state = session_state
                initial_state.user_input = user_message
                initial_state.dict_of_prompts = GLOBAL_PROMPTS
                initial_state.case_id = current_case_id or initial_state.case_id
            else:
                initial_state = IOState(
                    case_id=current_case_id,
                    user_input=user_message,
                    dict_of_prompts=GLOBAL_PROMPTS
                )

            # Run Agent
            result = await asyncio.to_thread(io_agent.invoke, initial_state)
            
            # Store result back into session state for next turn
            session_state = IOState(**result)
            
            # Check Intent
            if not result.get("intent_valid", False):
                 response_message = result.get("agent_response", "I didn't understand that. Please say 'start' to begin.")
                 await manager.send_personal_message(json.dumps({
                    "type": "response",
                    "success": True,
                    "response": response_message
                }), websocket)
                 continue

            # If awaiting file selection, skip default success message (handled by node)
            if result.get("awaiting_file_selection"):
                logger.info("Awaiting file selection. Skipping default success message.")
                continue
            
            # If awaiting selection (Campaign Selection), skip default success message
            if result.get("awaiting_selection"):
                logger.info("Awaiting campaign selection. Handled by node.")
                continue

            # Construct Response
            response_payload = {
                "type": "response",
                "success": True,
                "response": result.get("agent_response", "Extraction and Order Creation Completed!"),
                "created_records": {
                    "Order": [{"Id": result.get("order_id"), "Name": "New Order"}] if result.get("order_id") else []
                },
                "salesforce_data": True
            }
            
            if result.get("insertion_errors"):
                 response_payload["response"] = f"Completed with errors: "

            await manager.send_personal_message(json.dumps(response_payload), websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"Error in websocket: {e}")
        # Only try to send error message if the websocket is still connected
        try:
            if websocket in manager.active_connections:
                await manager.send_personal_message(json.dumps({
                    "type": "error",
                    "message": str(e)
                }), websocket)
        except Exception as send_error:
            logger.error(f"Failed to send error message to client: {send_error}")
        finally:
            # Clean up the connection if it's still tracked
            if websocket in manager.active_connections:
                manager.disconnect(websocket)

# --- HTTP Endpoint (Legacy/Backup) ---
class StartRequest(BaseModel):
    message: str
    file_path: str = "output8.md"

@app.post("/start", response_model=AgentResponse)
async def start_agent(request:AgentRequest ):
    # Remote agent entry point
    # return {"status": "Use WebSocket /ws for interaction"}
    # """
    # HTTP entry point for Orchestrator.
    # Maps AgentRequest to IO Agent execution.
    # """
    logger.info(f"🚀 IO Agent received HTTP request: {request}")
    
    # Determine session_id
    session_id = request.session_id or (request.context.get("session_id") if request.context else None)
    
    # 1. Start with fresh state
    initial_state_dict = {}

    # 2. Try to restore from file-based session (Top Priority for Persistence)
    if session_id:
        initial_state_dict = _load_session(session_id)
    
    # 3. Merge ONLY essential fields from request context
    # DO NOT blindly merge PacePal context — it contains fields like 'error: None'
    # that conflict with IOState fields (e.g. error_flag expects bool, not None)
    if request.context:
        # Only merge fields that are actually relevant and non-None
        safe_context_fields = ['record_id', 'session_id', 'case_id']
        for key in safe_context_fields:
            if key in request.context and request.context[key] is not None:
                initial_state_dict[key] = request.context[key]
        # Map record_id to case_id if case_id not already set
        if not initial_state_dict.get('case_id') and request.context.get('record_id'):
            initial_state_dict['case_id'] = request.context['record_id']
        
    # 4. Initialize IOState
    try:
        if initial_state_dict:
            initial_state = IOState.model_validate(initial_state_dict)
        else:
            initial_state = IOState(
                case_id=request.context.get("record_id") if request.context else None
            )
    except Exception as e:
        logger.error(f"Failed to validate state: {e}. Trying session-only data.")
        # Try session data WITHOUT context merge
        if session_id:
            raw_session = _load_session(session_id)
            if raw_session:
                try:
                    initial_state = IOState.model_validate(raw_session)
                    logger.info("✅ Recovered state from session-only data.")
                except Exception as e2:
                    logger.error(f"Session-only validation also failed: {e2}. Using fresh state.")
                    initial_state = IOState(case_id=request.context.get("record_id") if request.context else None)
            else:
                initial_state = IOState(case_id=request.context.get("record_id") if request.context else None)
        else:
            initial_state = IOState(case_id=request.context.get("record_id") if request.context else None)

    # 5. Always update with current user input/goal
    initial_state.user_input = request.resume if request.resume else request.user_goal
    initial_state.dict_of_prompts = GLOBAL_PROMPTS
    
    # Handle Resume logic
    if request.resume and initial_state.awaiting_selection:
        logger.info("Resuming from user selection...")
        initial_state.awaiting_selection = False
        initial_state.user_selection = request.resume

    try:
        # Run Agent
        result = await asyncio.to_thread(io_agent.invoke, initial_state)
        
        # Save updated state to session storage
        if session_id:
            _save_session(session_id, result)
        
        # Determine status
        status = "success"
        if result.get("awaiting_selection"):
            status = "interrupted"
        elif result.get("error_flag") or result.get("insertion_errors"):
            status = "error"
        
        # Construct Response
        return AgentResponse(
            status=status,
            result=result,
            final_response=result.get("agent_response", "IO Processing completed."),
            created_records={
                "Order": [{"Id": result.get("order_id"), "Name": "New Order"}] if result.get("order_id") else []
            } if result.get("order_id") else None,
            # Robust mapping of errors
            error=str(result.get("insertion_errors") or result.get("error_message") or "") if (result.get("insertion_errors") or result.get("error_flag")) else None
        )
        
    except Exception as e:
        logger.error(f"❌ IO Agent execution error: {e}")
        return AgentResponse(
            status="error",
            result={},
            error=str(e),
            final_response=f"Error executing IO Agent: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
