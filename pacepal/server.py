"""
PacePal Server - Top-level orchestrator server
Entry point for all client requests
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
# Add marketing agent directory to Python path to resolve its internal relative imports
sys.path.insert(0, str(project_root / "agents/marketing"))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command
from dotenv import load_dotenv
from core.helper import preload_mcp_tools
from core.helper import get_member_dependency, preload_prompts_for_consumer
from pacepal.graph import build_pacepal_graph
from shared.config import PACEPAL_PORT, DEPLOYMENT_MODE
import ast
import logging
import uuid
import json
import sys

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pacepal.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger(__name__)

app = FastAPI(title="PacePal Orchestrator Server", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global memory saver to persist state across WebSocket connections
memory = MemorySaver()
agent_graph = build_pacepal_graph(checkpointer=memory)

logger.info("✅ PacePal graph initialized with persistent memory")


@app.on_event("startup")
async def startup_event():
    """Pre-load MCP tools on server startup"""
    try:
        logger.info(f"🚀 Starting PacePal Server (mode: {DEPLOYMENT_MODE})...")
        
        # Preload prompts for PacePal Agent (System Prompts)
        logger.info("📦 Preloading prompts for PacePal Agent...")
        preload_prompts_for_consumer("Pacepal Agent")
        
        # Preload dependencies for PacePal Agent (System Dependencies)
        logger.info("📦 Preloading member dependencies for Pacepal Agent...")
        get_member_dependency("Pacepal Agent")

        # Only preload MCPs in monolith mode
        # In distributed mode, each agent handles its own MCPs
        if DEPLOYMENT_MODE == "monolith":
            logger.info("📦 Preloading MCP tools (monolith mode)...")
            registry = get_member_dependency("Marketing Agent")
            
            service_configs = {}
            for name, data in registry.items():
                if data.get("executionEndpoint"):
                    config = {
                        "command": "python",
                        "args": data.get("executionEndpoint"),
                        "env": None
                    }
                    if isinstance(config["args"], str):
                        try:
                            config["args"] = json.loads(config["args"])
                        except:
                            config["args"] = [config["args"]]
                    service_configs[name] = config
            
            if service_configs:
                await preload_mcp_tools(service_configs)
            else:
                logger.warning("⚠️ No valid MCP configurations found")
        else:
            logger.info("🌐 Distributed mode - MCPs handled by individual agents")
            
    except Exception as e:
        logger.error(f"❌ Error during startup: {e}")


@app.websocket("/ws/chat")
async def run_agent(websocket: WebSocket):
    """WebSocket endpoint for real-time agent communication"""
    await websocket.accept()
    
    logger.info(f"🔌 New WebSocket connection established")

    try:
        while True:
            # Receive message
            data = await websocket.receive_text()
            logger.info(f"📨 [WebSocket] Received raw data: {data[:100]}...")
            
            message_data = json.loads(data)
            msg_type = message_data.get("type", "message")
            user_message = message_data.get("message", "")
            record_id = message_data.get("recordId")
            
            # Accept session_id from client, or generate new one if not provided
            session_id = message_data.get("session_id")
            if not session_id:
                session_id = str(uuid.uuid4())
                logger.info(f"🆕 Generated new session ID: {session_id}")
            else:
                logger.info(f"🔄 Using existing session ID: {session_id}")
            
            thread_config = {"configurable": {"thread_id": session_id}}
            
            # Handle Connection Init (Store record context)
            if msg_type == "connection_init":
                logger.info(f"🔌 [WebSocket] Connection Init received. Session ID: {session_id}, Record ID: {record_id}")
                update_data = {"session_id": session_id}
                if record_id:
                    update_data["record_id"] = record_id
                
                await agent_graph.aupdate_state(thread_config, update_data, as_node="pacepal_orchestrator")
                await websocket.send_json({"type": "status", "message": "Context initialized"})
                continue

            logger.info(f"📨 [WebSocket] User message: {user_message}")

            # Check if resuming from interrupt
            snapshot = await agent_graph.aget_state(thread_config)
            logger.info(f"🔍 [WebSocket] Snapshot next: {snapshot.next}")
            
            # Get existing record_id from state if not in this message
            if not record_id and snapshot.values:
                record_id = snapshot.values.get("record_id")
            
            final_state = {}
            
            # Execute graph (handle interrupts gracefully)
            from langgraph.errors import GraphInterrupt
            try:
                # 🔄 EXECUTION LOOP (Command Chaining)
                # Allows the agent to trigger a follow-up action naturally
                current_goal = user_message
                is_chaining_restart = False
                next_input_override = None
                
                while True: 
                    # Get fresh snapshot at start of loop if needed
                    if not is_chaining_restart:
                        snapshot = await agent_graph.aget_state(thread_config)
                    
                    # 🔍 DECIDE: Start new turn or Resume?
                    # Only use Command(resume=...) if there is a real interrupt waiting for a value
                    has_interrupt = snapshot.tasks and snapshot.tasks[0].interrupts if snapshot.tasks else False
                    
                    if has_interrupt and not is_chaining_restart:
                        # Resuming interrupted graph (waiting for user input)
                        logger.info(f"▶️ Resuming interrupted graph at {snapshot.next}. Input: {current_goal}")
                        res_command = Command(resume=current_goal, update={
                            "user_goal": current_goal, 
                            "resume_data": current_goal,
                            "session_id": session_id,
                            "record_id": record_id
                        })
                        final_state = await agent_graph.ainvoke(res_command, thread_config)
                    else:
                        # Starting new turn OR executing initialized but not started graph
                        logger.info(f"▶️ {'Restarting due to chaining' if is_chaining_restart else 'Starting new graph turn'}")
                        # Clear resume_data and reset context
                        initial_inputs = {
                            "user_goal": current_goal,
                            "session_id": session_id,
                            "record_id": record_id,
                            "resume_data": None # 🛡️ ENSURE Fresh Start for Agents
                        }
                        
                        if is_chaining_restart and next_input_override:
                             # Use the override input for the new run
                             await agent_graph.ainvoke(next_input_override, thread_config)
                             next_input_override = None
                             is_chaining_restart = False
                        else:
                             from langchain_core.messages import HumanMessage
                             
                             initial_input = {
                                **initial_inputs,
                                "messages": [HumanMessage(content=current_goal)],
                                # Clear transient state
                                "final_response": None,
                                "error": None,
                                "next_action": None,
                                "salesforce_data": None,
                                "mcp_results": None,
                                "active_workflow": None,
                                "task_directive": None,
                                "pending_updates": None,
                                "email_workflow_context": None,
                                "engagement_workflow_context": None,
                                "save_workflow_context": None,
                                "brevo_results": None,
                                "linkly_links": None,
                                "created_records": None,
                                "chain_command": None,
                                "shared_result_sets": snapshot.values.get("shared_result_sets", []) if snapshot.values else [], 
                                "session_context": {},
                            }
                            
                             # Preserve context if chaining (should not happen in this branch but good safety)
                             if "chain_command" in snapshot.values:
                                  initial_input["session_context"] = snapshot.values.get("session_context", {})
                                  initial_input["shared_result_sets"] = snapshot.values.get("shared_result_sets", [])
                                  initial_input["record_id"] = snapshot.values.get("record_id", record_id)
                            
                             await agent_graph.ainvoke(initial_input, thread_config)

                    # Get latest state
                    snapshot = await agent_graph.aget_state(thread_config)
                    final_state = snapshot.values

                    # Check for interrupts first
                    if snapshot.tasks and snapshot.tasks[0].interrupts:
                        break # Exit loop to handle interrupt
                        
                    # Check for chaining
                    chain_cmd = final_state.get("chain_command")
                    if chain_cmd:
                        logger.info(f"🔗 [Chaining] Logic detected chain command: '{chain_cmd}'")
                        
                        # Only send response if it's NOT just the chain acknowledgement
                        # (But usually we want to see "Cancelled. doing X")
                        
                        # Send current response BEFORE chaining
                        created_records_map = final_state.get("created_records", {}) or {}
                        filtered_records = {}
                        for obj_type, records in created_records_map.items():
                            valid_recs = [r for r in records if r.get("Name") and not r.get("Name").endswith(" Record")]
                            if valid_recs:
                                filtered_records[obj_type] = valid_recs
                        
                        final_resp = final_state.get("final_response", "Processing...")
                        
                        # 🔄 UI RESET: Tell frontend to disable previous buttons
                        await websocket.send_json({
                            "type": "chain_reset",
                            "message": "🔄 Switching context..."
                        })
                        
                        # REMOVED: Redundant response message that causes blank bubbles
                        # The UI shows "Switching context..." which is sufficient.
                        
                        # Update goal and continue loop
                        current_goal = chain_cmd
                        logger.info(f"🔄 [Chaining] Restarting loop with new goal: {current_goal}")
                        
                        # Prepare input for next run
                        next_input = {
                            "user_goal": current_goal,
                            "messages": [HumanMessage(content=current_goal)],
                            "session_id": session_id,
                            
                            # Clear transient state
                            "final_response": None,
                            "error": None,
                            "next_action": None,
                            "salesforce_data": None,
                            "mcp_results": None,
                            "active_workflow": None,
                            "task_directive": None,
                            "resume_data": None,
                            "pending_updates": None,
                            "email_workflow_context": None,
                            "engagement_workflow_context": None,
                            "save_workflow_context": None,
                            "brevo_results": None,
                            "linkly_links": None,
                            "created_records": None,
                            "chain_command": None, # Ensure cleared
                            # Clear persistent result sets and context for new request (fresh start)
                            "shared_result_sets": final_state.get("shared_result_sets", []), 
                            "session_context": final_state.get("session_context", {})
                        }
                        
                        # Set override input for next iteration
                        next_input_override = next_input
                        is_chaining_restart = True
                        continue
                    
                    # If no chaining and no interrupt, we are done
                    break
            except GraphInterrupt:
                logger.info("⏸️ Graph execution interrupted (caught exception)")
                # Continue to process snapshot
            
            snapshot = await agent_graph.aget_state(thread_config)
            final_state = snapshot.values
                
            if snapshot.tasks and snapshot.tasks[0].interrupts:
                logger.info("🛑 Graph execution interrupted")
                interrupt_obj = snapshot.tasks[0].interrupts[0]
                
                # Handle both object with .value attribute and direct value
                if hasattr(interrupt_obj, "value"):
                    interrupt_value = interrupt_obj.value
                else:
                    interrupt_value = interrupt_obj
                    
                # Unwrap list if it's a list (common with interrupt([val]))
                if isinstance(interrupt_value, list) and len(interrupt_value) > 0:
                    interrupt_value = interrupt_value[0]
                    
                # Unwrap Interrupt object if deeply nested
                if hasattr(interrupt_value, "value"):
                    interrupt_value = interrupt_value.value
                
                # Debug logging
                logger.info(f"🔍 Interrupt Value Type: {type(interrupt_value)}")
                logger.info(f"🔍 Interrupt Value Content: {interrupt_value}")

                # 1. If it's a dict with a 'type', send it directly (Monolith / Local)
                if isinstance(interrupt_value, dict) and "type" in interrupt_value:
                     logger.info("✅ Sending interrupt as DICT")
                     await websocket.send_json(interrupt_value)
                

                # 2. If it's a string, try to parse as JSON (Distributed)
                elif isinstance(interrupt_value, str):
                    parsed = None
                    try:
                        # First try standard JSON
                        parsed = json.loads(interrupt_value)
                    except json.JSONDecodeError:
                        # Fallback: Try ast.literal_eval for Python-style strings (single quotes)
                        try:
                            parsed = ast.literal_eval(interrupt_value)
                        except (ValueError, SyntaxError):
                            pass
                    
                    if isinstance(parsed, dict) and "type" in parsed:
                        logger.info("✅ Sending interrupt as PARSED OBJECT")
                        await websocket.send_json(parsed)
                    else:
                        # Not a control message, send as text
                        logger.warning(f"⚠️ Parsed value missing 'type' or not a dict: {parsed}")
                        # Fallback: Send as standard text response
                        await websocket.send_json({
                            "type": "response",
                            "success": True,
                            "response": interrupt_value,
                            "iterations": 0,
                            "salesforce_data": False,
                            "created_records": {},
                            "error": None
                        })
                
                # 3. Fallback for other types
                else:
                    await websocket.send_json({
                        "type": "response",
                        "success": True,
                        "response": str(interrupt_value),
                        "iterations": 0,
                        "salesforce_data": False,
                        "created_records": {},
                        "error": None
                    })
                continue
            
            # Process result
            created_records_map = final_state.get("created_records", {}) or {}
            filtered_records = {}
            for obj_type, records in created_records_map.items():
                valid_recs = [r for r in records if r.get("Name") and not r.get("Name").endswith(" Record")]
                if valid_recs:
                    filtered_records[obj_type] = valid_recs
            
            final_resp = final_state.get("final_response", "Task completed")
            
            # Send response
            if isinstance(final_resp, str) and ('\"type\":' in final_resp):
                try:
                    await websocket.send_json(json.loads(final_resp))
                except json.JSONDecodeError:
                    # Fallback if it looks like JSON but isn't
                    await websocket.send_json({
                        "type": "response",
                        "success": True,
                        "response": final_resp,
                        "iterations": final_state.get("iteration_count", 0),
                        "salesforce_data": bool(final_state.get("salesforce_data")),
                        "created_records": filtered_records,
                        "generated_email_content": final_state.get("generated_email_content"),
                        "structured_summary": final_state.get("structured_summary"),
                        "error": final_state.get("error")
                    })
            else:
                await websocket.send_json({
                    "type": "response",
                    "success": True,
                    "response": final_resp,
                    "iterations": final_state.get("iteration_count", 0),
                    "salesforce_data": bool(final_state.get("salesforce_data")),
                    "created_records": filtered_records,
                    "generated_email_content": final_state.get("generated_email_content"),
                    "structured_summary": final_state.get("structured_summary"),
                    "error": final_state.get("error")
                })
            
    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"Server Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await websocket.send_json({
            "type": "error",
            "message": str(e)
        })


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "pacepal",
        "deployment_mode": DEPLOYMENT_MODE,
        "version": "1.0.0"
    }


@app.get("/")
async def root():
    """Root endpoint with service info"""
    return {
        "service": "PacePal Orchestrator Server",
        "version": "1.0.0",
        "deployment_mode": DEPLOYMENT_MODE,
        "endpoints": {
            "websocket": "/ws/chat",
            "health": "/health (GET)"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"🚀 Starting PacePal Server on port {PACEPAL_PORT}")
    logger.info(f"📝 Deployment mode: {DEPLOYMENT_MODE}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PACEPAL_PORT,
        log_config=None
    )
