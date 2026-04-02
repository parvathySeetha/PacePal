"""
Standalone Marketing Agent Server
Can be run independently or as part of the monolith
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from shared.models import AgentRequest, AgentResponse
from agents.marketing.graph import build_marketing_graph  # Use local graph
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command
import logging
import sys
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("marketing_agent.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Marketing Agent Server", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize graph with memory (one instance for the server)
memory = MemorySaver()
marketing_graph = build_marketing_graph(checkpointer=memory)

logger.info("✅ Marketing Agent Server initialized")


@app.on_event("startup")
async def startup_event():
    """Pre-load MCP tools and prompts on server startup"""
    try:
        logger.info("📦 Preloading MCP tools for Marketing Agent...")
        
        # Import here to avoid circular dependencies
        from core.helper import get_member_dependency, preload_prompts_for_consumer
        from core.helper import preload_mcp_tools
        
        # ✅ Preload prompts for Marketing Agent
        logger.info("🔄 Preloading prompts for Marketing Agent...")
        try:
            success = preload_prompts_for_consumer("Marketing Agent")
            if success:
                logger.info("✅ Prompt cache initialized successfully")
            else:
                logger.warning("⚠️ Prompt cache initialization failed, will fetch on-demand")
        except Exception as e:
            logger.error(f"❌ Error during prompt preload: {e}")
            logger.warning("⚠️ Continuing without cache, will fetch prompts on-demand")
        
        # Get registry which contains configs for all MCPs
        registry = get_member_dependency("Marketing Agent")
        
        service_configs = {}
        for name, data in registry.items():
            if data.get("executionEndpoint"):
                config = {
                    "command": "python",
                    "args": data.get("executionEndpoint"),
                    "env": None
                }
                # Parse args if it's a JSON string
                if isinstance(config["args"], str):
                    try:
                        config["args"] = json.loads(config["args"])
                    except:
                        config["args"] = [config["args"]]
                
                service_configs[name] = config
        
        if service_configs:
            logger.info(f"   Found {len(service_configs)} MCP services to preload")
            await preload_mcp_tools(service_configs)
            logger.info("✅ MCP tools preloaded successfully")
        else:
            logger.warning("⚠️ No valid MCP configurations found")
            
    except Exception as e:
        logger.error(f"❌ Error during MCP preloading: {e}")
        import traceback
        logger.error(traceback.format_exc())


@app.post("/execute", response_model=AgentResponse)
async def execute_marketing_task(request: AgentRequest):
    """
    Execute a marketing agent task
    
    Args:
        request: AgentRequest with user_goal, context, session_id, messages
        
    Returns:
        AgentResponse with status, result, and optional fields
    """
    try:
        logger.info(f"    Session ID: {request.session_id}")
        
        if request.resume is not None:
             logger.info(f"🔄 Marketing Agent received RESUME request")
        else:
             logger.info(f"📧 Marketing Agent received NEW task: {request.user_goal[:50]}...")
             
        # Initialize thread config
        thread_config = {"configurable": {"thread_id": request.session_id}}
        
        # Prepare initial state from request
        from langchain_core.messages import HumanMessage
        
        initial_state = {
            "user_goal": request.user_goal,
            "messages": [HumanMessage(content=request.user_goal)],
            # Include all context
            **request.context
        }
        
        # Execute graph with session-specific thread
        thread_config = {"configurable": {"thread_id": request.session_id}}
        
        # 🔄 EXECUTION LOOP (Command Chaining)
        current_input = request.resume if request.resume is not None else initial_state
        is_resume = request.resume is not None
        
        while True:
            if is_resume:
                logger.info(f"▶️ Resuming graph with input: {current_input}")
                # Resume the graph with the provided value using Command
                # If we are chaining, we might need to update state too
                if isinstance(current_input, dict) and "chain_command" in current_input:
                     # This is a restart, not a resume
                     logger.info("  (Restarting due to chain)")
                     result = await marketing_graph.ainvoke(current_input, thread_config)
                else:
                    result = await marketing_graph.ainvoke(Command(resume=current_input), thread_config)
            else:
                logger.info("▶️ Starting new graph turn")
                result = await marketing_graph.ainvoke(current_input, thread_config)
            
            # Check for interrupts
            snapshot = await marketing_graph.aget_state(thread_config)
            interrupt_value = None
            
            if snapshot.tasks and snapshot.tasks[0].interrupts:
                 val = snapshot.tasks[0].interrupts[0].value
                 # Ensure we serialize to valid JSON string
                 if isinstance(val, (dict, list)):
                     interrupt_value = json.dumps(val)
                 else:
                     interrupt_value = str(val)
                 
                 # Check if this interrupt is actually a CHAIN command?
                 # No, the interrupt happens at the node level.
                 # The chain command is returned as a specific key in state.
                 
                 status = "interrupted"
                 # We break here to let user handle interrupt
                 break
            else:
                # Graph finished this turn
                logger.info(f"✅ Marketing graph completed (turn)")
                
                # CHECK FOR CHAINING
                # Retrieve final state
                final_state = snapshot.values
                chain_cmd = final_state.get("chain_command")
                
                if chain_cmd:
                    logger.info(f"🔗 [Chaining] Logic detected chain command: '{chain_cmd}'")
                    # 🛑 STOP: Do not restart locally. Return to Orchestrator.
                    # This allows PacePal server to see the chain command and send "chain_reset" to UI.
                    status = "success"
                    break
                
                status = "success"
                break
        
        logger.debug(f"   Result keys: {list(result.keys())}")
        result = snapshot.values # Ensure we get latest state
        
        # Extract response fields
        # Helper: Extract records from salesforce_data if created_records is missing (for updates)
        created_recs = result.get("created_records")
        if not created_recs and result.get("salesforce_data"):
            sf_data = result.get("salesforce_data")
            # Handle list or dict
            if isinstance(sf_data, dict): sf_data = [sf_data]
            if isinstance(sf_data, list):
                extracted = []
                for item in sf_data:
                    # Look for successful operations with Id/Name
                    if isinstance(item, dict) and item.get("success") and item.get("id"):
                        # Try to find Name in original request or use ID or Type
                        # We might need to construct a minimal record object
                        rec_id = item.get("id")
                        # Attempt to find more context if available, otherwise just ID
                        extracted.append({
                            "Id": rec_id,
                            "Name": item.get("Name") or f"Record ({rec_id})", # Fallback name
                            "Email": item.get("Email")
                        })
                if extracted:
                    created_recs = {"Updated Records": extracted}

        return AgentResponse(
            status=status,
            result=result,
            next_action=result.get("next_action"),
            final_response=result.get("final_response", "Task completed") if not interrupt_value else interrupt_value,
            error=result.get("error"),
            created_records=created_recs,
            generated_email_content=result.get("generated_email_content")
        )
        
    except Exception as e:
        logger.exception(f"❌ Marketing agent error: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Marketing agent execution failed: {str(e)}"
        )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "agent": "marketing",
        "version": "1.0.0"
    }


@app.get("/")
async def root():
    """Root endpoint with service info"""
    return {
        "service": "Marketing Agent Server",
        "version": "1.0.0",
        "endpoints": {
            "execute": "/execute (POST)",
            "health": "/health (GET)"
        }
    }


if __name__ == "__main__":
    import uvicorn
    from shared.config import MARKETING_PORT
    
    logger.info(f"🚀 Starting Marketing Agent Server on port {MARKETING_PORT}")
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=MARKETING_PORT,
        log_config=None  # Use our logging config
    )
