"""
Standalone Reconciliation Agent Server
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from shared.models import AgentRequest, AgentResponse
from agents.Reconciliation.graph import build_reconcillation_graph
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command
import logging
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("reconcillation_agent.log", mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Reconciliation Agent Server", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize graph with memory
memory = MemorySaver()
reconcillation_graph = build_reconcillation_graph(checkpointer=memory)

logger.info("✅ Reconciliation Agent Server initialized")

@app.on_event("startup")
async def startup_event():
    """Pre-load MCP tools and prompts on server startup"""
    try:
        logger.info("📦 Preloading MCP tools for Reconciliation Agent...")
        
        from core.helper import get_member_dependency, preload_prompts_for_consumer
        from core.helper import preload_mcp_tools
        
        # Preload prompts
        logger.info("🔄 Preloading prompts for Reconciliation Agent...")
        try:
            success = preload_prompts_for_consumer("Reconciliation Agent")
            if success:
                logger.info("✅ Prompt cache initialized successfully")
            else:
                logger.warning("⚠️ Prompt cache initialization failed")
        except Exception as e:
            logger.error(f"❌ Error during prompt preload: {e}")
        
        # Get registry which contains configs for all MCPs
        registry = get_member_dependency("Reconciliation Agent")
        
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
            logger.info(f"   Found {len(service_configs)} MCP services to preload")
            await preload_mcp_tools(service_configs)
            logger.info("✅ MCP tools preloaded successfully")
            
    except Exception as e:
        logger.error(f"❌ Error during MCP preloading: {e}")

@app.post("/execute", response_model=AgentResponse)
async def execute_reconcillation_task(request: AgentRequest):
    """
    Execute a reconciliation agent task
    """
    try:
        logger.info(f"    Session ID: {request.session_id}")
        
        if request.resume is not None:
             logger.info(f"🔄 Reconciliation Agent received RESUME request")
        else:
             logger.info(f"📧 Reconciliation Agent received NEW task: {request.user_goal[:50]}...")
             logger.info(f"   Starting new graph turn {list(request.context.keys())}")
             logger.info(f"   Record ID: {request.context.get('record_id')}")
             
        # Initialize thread config
        thread_config = {"configurable": {"thread_id": request.session_id}}
        
        from langchain_core.messages import HumanMessage
        
        initial_state = {
            "user_goal": request.user_goal,
            "messages": [HumanMessage(content=request.user_goal)],
            **request.context
        }
        
        # EXECUTION LOOP
        current_input = request.resume if request.resume is not None else initial_state
        is_resume = request.resume is not None
        
        while True:
            if is_resume:
                logger.info(f"▶️ Resuming graph")
                result = await reconcillation_graph.ainvoke(Command(resume=current_input), thread_config)
            else:
                logger.info("▶️ Starting new graph turn")
                result = await reconcillation_graph.ainvoke(current_input, thread_config)
            
            # Check for interrupts
            snapshot = await reconcillation_graph.aget_state(thread_config)
            interrupt_value = None
            
            if snapshot.tasks and snapshot.tasks[0].interrupts:
                 val = snapshot.tasks[0].interrupts[0].value
                 if isinstance(val, (dict, list)):
                     interrupt_value = json.dumps(val)
                 else:
                     interrupt_value = str(val)
                 
                 status = "interrupted"
                 break
            else:
                logger.info(f"✅ Reconciliation graph completed (turn)")
                final_state = snapshot.values
                status = "success"
                break
        
        result = snapshot.values
        
        return AgentResponse(
            status=status,
            result=result,
            next_action=result.get("next_action"),
            final_response=result.get("final_response", "Task completed") if not interrupt_value else interrupt_value,
            error=result.get("error"),
            created_records=result.get("created_records"),
            structured_summary=result.get("structured_summary")
        )
        
    except Exception as e:
        logger.exception(f"❌ Reconciliation agent error: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Reconciliation agent execution failed: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "agent": "reconcillation",
        "version": "1.0.0"
    }

@app.get("/")
async def root():
    """Root endpoint with service info"""
    return {
        "service": "Reconciliation Agent Server",
        "version": "1.0.0",
        "endpoints": {
            "execute": "/execute (POST)",
            "health": "/health (GET)"
        }
    }

if __name__ == "__main__":
    import uvicorn
    # Use 8003 as requested
    PORT = 8003
    
    logger.info(f"🚀 Starting Reconciliation Agent Server on port {PORT}")
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=PORT,
        log_config=None
    )
