
# OG 
from dotenv import load_dotenv
load_dotenv()
import json
from functools import lru_cache
from typing import Dict, List, Any ,Optional
from dataclasses import dataclass
import os
from openai import AsyncOpenAI
from langchain_openai import ChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
import asyncio
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from mcp_module.Salesforcemcp.client.sf_client import SalesforceClient
import logging
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from agents.marketing.state import MarketingState
import sys
import re

# ps gemma- remove later
import os
import httpx

# Ensure env vars are loaded
load_dotenv(override=True)

try:
    from vault_utils import read_secret
except ImportError:
    # Fallback if vault_utils not in path (though it should be)
    logger = logging.getLogger(__name__)
    logger.warning("Could not import vault_utils, Vault features will be disabled.")
    def read_secret(path, mount="secret"): return {}


_sf_connected=False
logger = logging.getLogger(__name__)
# Choose the org that hosts these MemberDefinition records
SF_ORG_TYPE = "agent" 
sf_client = SalesforceClient(SF_ORG_TYPE)

# Prompt cache infrastructure
_prompt_cache: Dict[str, Dict[str, Any]] = {}
_cache_initialized: bool = False

@dataclass
class PromptConfig:
    """Represents a single prompt configuration item"""
    name: str
    config_type: str
    placeholder_name: Optional[str] = None
    data_type: Optional[str] = None
    is_required: bool = False
    default_value: Optional[str] = None
    description: Optional[str] = None
    tool_name: Optional[str] = None
    source_type: Optional[str] = None
    state_path: Optional[str] = None  # New: path in state like "user_goal" or "results.salesforce"

def _load_planning_config(input_schema: Dict[str, Any], mcp_name: str) -> Dict[str, Any]:
    """
    Extract planning configuration from InputSchema__c.
    Falls back to defaults if _planning section missing.
    """
    planning = input_schema.get("_planning", {})
    
    return {
        "strategy": planning.get("strategy", "llm_planner"),
        "tool_name": planning.get("tool_name"),
        "required_context": planning.get("required_context", []),
        "prompt_template": planning.get("prompt_template"), # Name of Salesforce PromptTemplate
    }

def ensure_sf_connected(client=None):
    """Ensure Salesforce is connected (call this at runtime, not import time)"""
    if client:
        return client.connect()
    global _sf_connected
    if not _sf_connected:
        _sf_connected = sf_client.connect()
    return _sf_connected

import certifi
from pymongo import MongoClient

# MongoDB Connection
_mongo_client = None

def get_mongo_credentials():
    """
    Retrieve MongoDB credentials from Vault, falling back to environment variables.
    Returns:
        tuple: (mongo_uri, db_name)
    """
    mongo_uri = None
    db_name_full = None

    # 1. Try Vault
    try:
        # User has a typo in Vault "mongdb", so try both
        vault_secret = read_secret("mongodb") 
        if not vault_secret:
             vault_secret = read_secret("mongdb") # Typo fallback
             
        if vault_secret:
            mongo_uri = vault_secret.get("MONGO_URI")
            db_name_full = vault_secret.get("MONGO_DB_NAME")
            if mongo_uri:
                 logger.info("🔑 Retrieved MongoDB credentials from Vault")
    except Exception as e:
        logger.warning(f"⚠️ Failed to read mongodb secret from Vault: {e}")

    # 2. Fallback to Env
    if not mongo_uri:
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
             # Debugging: Print env keys to see if MONGO_URI is actually there (obscured)
             env_keys = [k for k in os.environ.keys() if "MONGO" in k]
             logger.warning(f"⚠️ MONGO_URI not found in env. Available MONGO keys: {env_keys}")
    
    if not db_name_full:
        db_name_full = os.getenv("MONGO_DB_NAME", "Cluster0")
        
    db_name = db_name_full.strip().split(" ")[0] if db_name_full else "Cluster0"
    
    return mongo_uri, db_name

def load_api_keys_from_vault():
    """Load API keys (OpenAI, Google, LangChain) from Vault into environment variables."""
    try:
        # Try generic 'api_keys' path
        secrets = read_secret("api_keys")
        if secrets:
            for key, val in secrets.items():
                if val:
                    os.environ[key] = str(val)
                    if key in ["OPENAI_API_KEY", "GOOGLE_API_KEY", "LANGCHAIN_API_KEY", "LANGCHAIN_TRACING_V2", "LANGCHAIN_ENDPOINT", "LANGCHAIN_PROJECT"]:
                        logger.info(f"🔑 Loaded {key} from Vault")
        else:
            logger.info("ℹ️ No 'api_keys' secret found in Vault")
            
    except Exception as e:
        logger.warning(f"⚠️ Failed to load API keys from Vault: {e}")

# Call immediately to ensure env vars are set before other modules use them
load_api_keys_from_vault()

def get_mongo_client():
    """Get or create MongoDB client"""
    global _mongo_client
    if not _mongo_client:
        mongo_uri, _ = get_mongo_credentials()
        
        if not mongo_uri:
            logger.warning("MONGO_URI not set (checked Vault and Env)")
            return None
        try:
            _mongo_client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
            # Test connection
            _mongo_client.admin.command('ping')
            logger.info("✅ Connected to MongoDB successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            _mongo_client = None
    return _mongo_client

load_dotenv()

def _safe_json_loads(value: Any) -> Any:
    """Mongo stores schema sometimes as dict, sometimes as string JSON."""
    if value is None:
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}

@lru_cache
def _load_agent_member_dependency_mongo(parent_member: str) -> Dict[str, Any]:
    mongo_uri, db_name = get_mongo_credentials()
    
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is missing (checked Vault and Env)")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000, tlsCAFile=certifi.where())
    db = client[db_name]
    coll = db["MemberDependencies"]

    print(f"Querying {db_name}.MemberDependencies for parentMember.name = '{parent_member}'")
    
    # Find the parent member doc by name
    # Using the user's logic exactly
    doc = coll.find_one({"parentMember.name": parent_member})
    
    # Adding a debug print here to see if doc is found
    if doc:
        print("✅ Document FOUND!")
    else:
        print("❌ Document NOT FOUND with parentMember.name")
        # Try checking capitalization just in case, for debug
        doc2 = coll.find_one({"parentMember.Name": parent_member})
        if doc2:
             print("⚠️ Document found with parentMember.Name (Upper N)")
        
        doc3 = coll.find_one({"name": parent_member})
        if doc3:
             print("⚠️ Document found with name (root level)")

    if not doc:
        # Fallback to Title Case 'Name'
        print(f"⚠️ 'parentMember.name' not found. Trying 'parentMember.Name'...")
        doc = coll.find_one({"parentMember.Name": parent_member})
    
    if not doc:
        # Fallback to root level 'name'
        print(f"⚠️ 'parentMember.Name' not found. Trying root 'name'...")
        doc = coll.find_one({"name": parent_member})

    if not doc:
        # If still not found, try without strict equality for 'Marketing Agent' specifically or similar?
        # For now, strict match failure
        raise RuntimeError(f"No dependency document found in Mongo for parent member '{parent_member}'")

    dependent_members: List[Dict[str, Any]] = doc.get("dependentMembers", [])
    print(f"Found {len(dependent_members)} dependent members.")

    registry: Dict[str, Any] = {}
    for dm in dependent_members:
        name = dm.get("name")
        if not name:
            continue

        input_schema_json = _safe_json_loads(dm.get("inputSchema"))
        output_schema_json = _safe_json_loads(dm.get("outputSchema"))

        # If you store planning config inside inputSchema under "_planning"
        planning = (input_schema_json or {}).get("_planning", {}) if isinstance(input_schema_json, dict) else {}

        registry[name] = {
            "name": name,
            "entity_type": dm.get("entityType"),
            "description": dm.get("description"),
            "executionEndpoint": dm.get("executionEndpoint"),
            "intent": dm.get("intent"),
            "status": dm.get("status"),
            "input_schema": input_schema_json,
            "output_schema": output_schema_json,

            # ✅ Dependencies metadata (this is what your orchestrator needs)
            "dependencies": [
                {
                    "id": dm.get("memberDependencyId"),
                    "parent_member": doc.get("parentMemberId") or doc.get("parentMember", {}).get("parentMemberId"),
                    "dependency_type": dm.get("dependencyType"),
                    "call_order": dm.get("callOrder"),
                    "condition": dm.get("condition"),
                    "member_id": dm.get("memberId"),
                    "member_dependency_id": dm.get("memberDependencyId"),
                }
            ],

            # ✅ Planning config (if present)
            "planning_strategy": planning.get("strategy"),
            "planning_tool_name": planning.get("tool_name"),
            "planning_prompt_template": planning.get("prompt_template"),
            "required_context": planning.get("required_context", []),
        }

    # Optional: sort by call_order if you want deterministic orchestration
    # (registry is a dict; sorting usually happens where you iterate)
    return registry


def load_agent_member_dependency(
    parent_member: str   
) -> Dict[str, Any]:
    """
    Public wrapper around the loader.
    Now uses MongoDB loader.
    """
    return _load_agent_member_dependency_mongo(parent_member)


def get_member_dependency(
    parent_member: str 
) -> Dict[str, Any]:
    """
    Return the full registry dict for the given parent_member.
    """
    return load_agent_member_dependency(parent_member)


def refresh_member_dependency(
    parent_member: str 
) -> Dict[str, Any]:
    """
    Clear cache for this combination and re-load from Salesforce.
    Useful if MemberDefinition__c or MemberDependency__c are updated at runtime.
    """
    _load_agent_member_dependency_cached.cache_clear()
    # After clearing, re-call with the desired combination
    return _load_agent_member_dependency_cached(parent_member)

def preload_prompts_for_consumer(consumer_name: str) -> bool:
    """
    Preload all prompts for a given consumer (e.g., "Marketing Agent") into cache.
    Now uses MongoDB source.
    """
    global _prompt_cache, _cache_initialized
    
    mongo_uri, db_name = get_mongo_credentials()
    
    if not mongo_uri:
        logger.error("MONGO_URI missing for prompt preload")
        return False
        
    try:
        logger.info(f"🔄 [MongoPreload] Preloading prompts for: {consumer_name}")
        
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
        db = client[db_name]
        coll = db["Prompt"]
        
        # Query all active prompts for this consumer
        query = {
            "template.consumerName": consumer_name,
            "status": "Active"
        }
        
        cursor = coll.find(query)
        docs = list(cursor)
        
        if not docs:
            logger.warning(f"⚠️ [MongoPreload] No active prompts found for {consumer_name}")
            return False
            
        logger.info(f"📦 [MongoPreload] Found {len(docs)} prompt documents")
        
        count = 0
        for doc in docs:
            node_name = doc.get("template", {}).get("consumerSubUnit")
            if not node_name:
                continue
                
            # Parse Configs
            raw_configs = doc.get("promptConfig", [])
            configs = []
            
            for rc in raw_configs:
                configs.append(PromptConfig(
                    name=rc.get("Name"),
                    config_type=rc.get("ConfigType"),
                    placeholder_name=rc.get("PlaceholderName"),
                    data_type=rc.get("DataType"),
                    is_required=rc.get("IsRequired", False),
                    default_value=rc.get("DefaultValue"),
                    description=rc.get("Description"),
                    tool_name=rc.get("ToolName"),
                    source_type=rc.get("SourceType")
                ))
            
            # Populate Cache
            _prompt_cache[node_name] = {
                "prompt": doc.get("promptText", ""),
                "model": doc.get("llm", {}).get("model", "gpt-4o"),
                "provider": doc.get("llm", {}).get("provider", "OpenAI"),
                "configs": configs
            }
            count += 1
            
        _cache_initialized = True
        logger.info(f"🎉 [MongoPreload] Successfully cached {count} prompts for {consumer_name}")
        logger.info(f"🎉 [MongoPreload] Prompts are : {_prompt_cache}")
        return True
        
    except Exception as e:
        logger.error(f"❌ [MongoPreload] Error preloading prompts: {e}", exc_info=True)
        return False


def refresh_prompt_cache(consumer_name: str ) -> bool:
    """
    Clear and reload the prompt cache.
    Useful for runtime updates without server restart.
    
    Args:
        consumer_name: Value of ConsumerName__c field
        
    Returns:
        True if successful, False otherwise
    """
    global _prompt_cache, _cache_initialized
    
    logger.info(f"🔄 Refreshing prompt cache for {consumer_name}")
    _prompt_cache.clear()
    _cache_initialized = False
    
    return preload_prompts_for_consumer(consumer_name)


@lru_cache(maxsize=1000)
def fetch_prompt_metadata_mongo(
    node_name: str, 
    consumer_name: str 
) -> Optional[Dict[str, Any]]:
    """
    Fetch prompt metadata from MongoDB.
    Query: template.consumerSubUnit = node_name AND template.consumerName = consumer_name
    """
    mongo_uri, db_name = get_mongo_credentials()
    
    if not mongo_uri:
        logger.error("MONGO_URI missing for prompt fetch")
        return None

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
        db = client[db_name]
        # Collection name might be "Prompt" or "Prompts". Using "Prompt" based on user snippet.
        # User snippet: "domainType": "Prompt"
        coll = db["Prompt"]
        
        query = {
            "template.consumerSubUnit": node_name,
            "template.consumerName": consumer_name,
            "status": "Active" # Assuming we only want active ones
        }
        
        logger.info(f"🔍 [MongoPrompt] Querying Prompt: {query}")
        doc = coll.find_one(query)
        
    
        if not doc:
            logger.warning(f"⚠️ [MongoPrompt] No prompt found for {node_name} ({consumer_name})")
            return None
        
        logger.info(f"✅ [MongoPrompt] Found prompt: {doc.get('versionName')} (ID: {doc.get('_id')})")
        
        # Parse Configs
        raw_configs = doc.get("promptConfig", [])
        configs = []
        
        for rc in raw_configs:
            # Map Mongo JSON keys to PromptConfig (using keys from user provided JSON)
            configs.append(PromptConfig(
                name=rc.get("Name"),
                config_type=rc.get("ConfigType"),
                placeholder_name=rc.get("PlaceholderName"),
                data_type=rc.get("DataType"),
                is_required=rc.get("IsRequired", False),
                default_value=rc.get("DefaultValue"),
                description=rc.get("Description"),
                tool_name=rc.get("ToolName"),
                source_type=rc.get("SourceType")
            ))
            
        return {
            "prompt": doc.get("promptText", ""),
            "model": doc.get("llm", {}).get("model", "gpt-4o"),
            "provider": doc.get("llm", {}).get("provider", "OpenAI"),
            "configs": configs
        }

    except Exception as e:
        logger.error(f"❌ [MongoPrompt] Error fetching prompt: {e}")
        return None

@lru_cache(maxsize=10000)
def fetch_prompt_metadata(
    node_name: str, 
    consumer_name: str 
) -> Optional[Dict[str, Any]]:
    """
    Fetch prompt metadata for a specific node.
    First checks cache, then queries MongoDB (replaces Salesforce).
    
    Args:
        node_name: Value of ConsumerSubUnitName (e.g., "marketing_orchestrator")
        consumer_name: Value of ConsumerName (e.g., "Marketing Agent")
        
    Returns:
        Dict with keys: prompt, model, provider, configs
    """
    # Check cache first
    if node_name in _prompt_cache:
        logger.debug(f"✅ Cache hit for node: {node_name}")
        return _prompt_cache[node_name]
    
    logger.info(f"⚠️ Cache miss for node: {node_name}, querying MongoDB...")
    
    result = fetch_prompt_metadata_mongo(node_name, consumer_name)
    
    if result:
         # Cache the result
        _prompt_cache[node_name] = result

        logger.info(f"PS123 PROMPTCACHE: {result}")
        
        return result
    
    return None


def resolve_placeholders(
    prompt: str,
    configs: List[PromptConfig],
    state: Dict[str, Any] 
) -> str:
    """
    Resolve all placeholders in the template text
    
    Args:
        prompt: Template with placeholders like {placeholder_name}
        configs: List of PromptConfig objects
        state: Current state dict (e.g., MarketingState)
        context: Additional context data
        
    Returns:
        Resolved template text
    """
    resolved_text = prompt
    
    
    # Build placeholder map
    placeholder_map = {}
    for config in configs:
        if config.config_type in ['Template Placeholder', 'Condition']:
            placeholder_name = config.placeholder_name or config.name
            value = _resolve_single_placeholder(config, state)
            placeholder_map[placeholder_name] = value
    
    # Replace placeholders
    for placeholder, value in placeholder_map.items():
        pattern = r'\{' + re.escape(placeholder) + r'\}'
        resolved_text = re.sub(pattern, str(value), resolved_text)
    
    # Check for unresolved required placeholders
    remaining_placeholders = re.findall(r'\{(\w+)\}', resolved_text)
    if remaining_placeholders:
        logger.warning(f"Unresolved placeholders: {remaining_placeholders}")
    
    return resolved_text

def _resolve_single_placeholder(
    config: PromptConfig,
    state: Dict[str, Any]
) -> Any:
    """
    Resolve a single placeholder based on its source type
    
    Args:
        config: PromptConfig object
        state: Current state
    Returns:
        Resolved value
    """
    source_type = config.source_type or 'literal'
    
    if source_type == 'literal':
        # Direct value from config
        return config.value or config.default_value or ''
    
    elif source_type == 'Context':
        # Get from state using state_path or placeholder_name
        path = config.state_path or config.placeholder_name
        return _get_nested_value(state, path, config.default_value)
    
    # elif source_type == 'Business service':
    #     # Call a tool/function to get value
    #     if config.tool_name:
    #         return _call_business_service(config.tool_name, state, context)
    #     return config.default_value or ''
    
    else:
        logger.warning(f"Unknown source_type: {source_type}")
        return config.default_value or ''

def _get_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Get nested value from dict using dot notation
    
    Example: 'results.salesforce.account_id' -> data['results']['salesforce']['account_id']
    """
    if not path:
        return default
    
    keys = path.split('.')
    value = data
    
    try:
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
            
            if value is None:
                return default
        return value
    except (KeyError, TypeError, AttributeError):
        return default


def build_llm(
    default_model: str,
    default_provider: str,
    default_temperature: float,
    api_key: Optional[str] = None,
):
    """
    Build an LLM client (OpenAI, Claude, Gemini) based on provider + model
    coming from PromptTemplateVersion__c.
    """
    provider = default_provider.lower()
    model = default_model.lower()
    temperature = default_temperature


    if provider == "openai":
        return ChatOpenAI(model=model, temperature=temperature, api_key=api_key)

    # elif provider == "anthropic":
    #     # e.g. model="claude-3-5-sonnet-20241022"
    #     return ChatAnthropic(model=model, temperature=temperature, api_key=api_key)

    elif provider == "gemini":
        # e.g. model="gemini-1.5-flash"
        api_key_to_use = api_key or os.getenv("GOOGLE_API_KEY")
        return ChatGoogleGenerativeAI(model=model, temperature=temperature, api_key=api_key_to_use)

    else:
        logger.warning(f"Unknown provider '{provider}', defaulting to OpenAI {default_model}")
        return ChatOpenAI(model=default_model, temperature=temperature, api_key=api_key)
 

# At module level
openai_async_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

async def call_llm(
    system_prompt: str,
    user_prompt: str,
    default_model: str,
    default_provider: str,
    default_temperature: float,
    api_key: Optional[str] = None,
) -> str:
    """
    Send a system + user prompt pair to the selected LLM and return the text content.
    Uses direct OpenAI client instead of LangChain to avoid async issues.
    """
    logger.info("🔵 Starting LLM call...")
    logger.info(f"🔵 Model: {default_model}, Provider: {default_provider}")
    
    provider = default_provider.lower()
    
    if provider == "openai":
        try:
            logger.info("🔵 Calling OpenAI API...")
            
            # Use the working OpenAI client
            client = AsyncOpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
            
            response = await client.chat.completions.create(
                model=default_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=default_temperature
            )
            
            content = response.choices[0].message.content
            logger.info(f"🔵 LLM call complete: {content[:100]}...")
            return content
            
        except Exception as e:
            logger.error(f"❌ OpenAI call failed: {e}", exc_info=True)
            raise
    
    else:
        # Fallback to LangChain for other providers
        logger.warning(f"Provider {provider} not directly supported, using LangChain")
        
        llm = build_llm(
            default_model=default_model,
            default_provider=default_provider,
            default_temperature=default_temperature,
            api_key=api_key,
        )

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]

        try:
            response = await asyncio.wait_for(llm.ainvoke(messages), 30.0)
            logger.info(f"🔵 LLM call complete: {response}")
            content = response.content
            if isinstance(content, list):
                # Some models return a list of parts (e.g. {'type': 'text', 'text': '...'})
                parts = [item.get("text", "") for item in content if isinstance(item, dict) and "text" in item]
                return "".join(parts)
            return str(content)
        except asyncio.TimeoutError:
            logger.error("❌ LLM call timed out")
            raise TimeoutError("LLM request timed out")

def build_mcp_server_params(config: Dict[str, Any]) -> StdioServerParameters:
    """
    Convert your MCP registry config (from Salesforce) into StdioServerParameters.
    Change the keys to match your actual metadata.
    """

    # Example 1: explicit fields
    command = config.get("command") or "python"
    args_raw = config.get("executionEndpoint") or []

    # Example 2: args stored as JSON string in Salesforce:
    # {"args": "[\"mcp_module/Salesforcemcp/sf_server.py\"]"}
    if isinstance(args_raw, str):
        try:
            args = json.loads(args_raw)
        except json.JSONDecodeError:
            args = [args_raw]
    else:
        args = args_raw

    return StdioServerParameters(
        command=command,
        args=args,
    )
    
    plan["calls"] = valid_calls
    logging.info(f"🎯 [{service_name}] Final plan: {len(valid_calls)} calls, needs_next={plan.get('needs_next_iteration')}")
    
    return plan


async def plan_mcp_execution(
    service_name: str,
    config: Dict[str, Any],
    tools_meta: List[Dict[str, Any]],
    state: MarketingState,
    session: ClientSession,
    iteration: int,
    previous_results: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Unified planning function - NO manual routing for different MCPs.
    """
    logging.info(f"[{service_name}] Planning execution...,{previous_results}")
    planning_strategy = config.get("planning_strategy", "llm_planner")
    
    # ✅ Check if required context is available
    required_context = config.get("required_context", [])
    # missing_context = []
    # for context_path in required_context:
    #     if not _get_nested_value(state, context_path):
    #         missing_context.append(context_path)
    
    # if missing_context:
    #     logging.warning(f"[{service_name}] Missing required context: {missing_context}")
    #     return {
    #         "calls": [],
    #         "needs_next_iteration": False,
    #         "needs_salesforce_data": True,
    #         "missing_context": missing_context
    #     }
    
    # ✅ Strategy 1: Internal Tool (e.g., Salesforce MCP)
    if planning_strategy == "internal_tool":
        tool_name = config.get("planning_tool_name")
        if not tool_name:
            logging.error(f"[{service_name}] Internal tool strategy but no tool_name")
            return {"calls": [], "needs_next_iteration": False}
        
        # Determine if we should call the planner
        should_plan = False
        query_to_use = ""
        
        if iteration == 1:
            # First iteration: use user goal
            should_plan = True
            query_to_use = state.get("user_goal", "")
            logging.info(f"[{service_name}] First iteration, using user goal as query")
        else:
            # ✅ FIX: Subsequent iterations - check for task directive
            task_directive = state.get("task_directive")
            if task_directive:
                should_plan = True
                query_to_use = task_directive
                logging.info(f"[{service_name}] Task directive found: {task_directive}")
                logging.info(f"[{service_name}] Will call planner again to handle directive")
            elif previous_results:
                # Work is done, no directive
                logging.info(f"[{service_name}] Internal tool completed with {len(previous_results)} results, no directive, stopping")
                return {"calls": [], "needs_next_iteration": False}
            else:
                # No previous results and no directive - something went wrong
                logging.warning(f"[{service_name}] Iteration {iteration} but no previous results and no directive, stopping")
                return {"calls": [], "needs_next_iteration": False}
        
        if should_plan:
            # ✅ Build context with ALL relevant information
            planning_context = {
                **state,
                "salesforcemcptool": tools_meta,
                "session_context": state.get("session_context", {}),
                "task_directive": state.get("task_directive"),
                "pending_updates": state.get("pending_updates"),
                "user_goal": state.get("user_goal"),
                "previous_results": previous_results,
                "iteration": iteration
            }
            
            gen_args = {"query": query_to_use, "context": planning_context}
            logging.info(f"[{service_name}] Calling internal planner: {tool_name}")
            logging.info(f"[{service_name}] Query: {query_to_use}")
            logging.info(f"[{service_name}] Context keys: {list(planning_context.keys())}")
            
            gen_result = await session.call_tool(tool_name, gen_args)
            plan_text = extract_json_response_from_tool_result(gen_result)
            logging.info(f"[{service_name}] Plan text: {plan_text}")
            
            if not plan_text:
                logging.warning(f"[{service_name}] Internal planner returned empty plan")
                return {"calls": [], "needs_next_iteration": False}
            
            try:
                plan = json.loads(plan_text)
                logging.info(f"[{service_name}] Parsed plan: {plan}")
                return plan
            except json.JSONDecodeError as e:
                logging.error(f"[{service_name}] Invalid plan JSON: {e}")
                return {"calls": [], "needs_next_iteration": False}
        else:
            return {"calls": [], "needs_next_iteration": False}
    
    # ✅ Strategy 2: LLM Planner (e.g., Brevo, Linkly)
    if planning_strategy == "llm_planner":
        # Get MCP-specific prompt from Salesforce using node name
        mcp_specific_prompt = ""
        node_name = config.get("planning_prompt_template")  # This is now a node name (ConsumerSubUnitName__c)
        
        if node_name:
            prompt_meta = fetch_prompt_metadata(node_name, "Marketing Agent")
            if prompt_meta:
                 # Resolve placeholders if needed
                 mcp_specific_prompt = resolve_placeholders(prompt_meta["prompt"], prompt_meta["configs"], state)
            else:
                 logging.warning(f"[{service_name}] Prompt for node '{node_name}' not found or empty.")
        
        tools_for_prompt = [
            {
                "name": t["name"],
                "description": t.get("description", ""),
                "schema": t.get("schema", {}),
            }
            for t in tools_meta
        ]
        logging.info(f"[{service_name}] Tools for prompt: {tools_for_prompt}")
        
        system_content = f"""You are a planner for '{service_name}'.

AVAILABLE TOOLS (name, description, schema):
{json.dumps(tools_for_prompt, indent=2)}

{mcp_specific_prompt}

EXECUTION STRATEGY:
- Plan ONLY ONE STEP at a time (one logical operation)
- Use "store_as": "name" to save results for later reference (e.g., "contacts", "campaign")
- Use "iterate_over": "name" to iterate over saved results (or "previous_result" for immediate previous)
- Use {{name.field}} to reference fields from other result sets
- Check if previous results are available before planning next steps

RESPONSE FORMAT (pure JSON, no comments):
{{
  "calls": [
    {{
      "tool": "<tool_name>",
      "reason": "<why>",
      "arguments": {{}},
      "store_as": null,
      "iterate_over": null
    }}
  ],
  "needs_next_iteration": true,
  "needs_salesforce_data": false
}}

Set "store_as": "name" to save results with a semantic name for cross-tool referencing.
Set "iterate_over": "previous_result" when the tool should run for EACH item from previous results.
Set "iterate_over": "name" to iterate over a specific named result set.
Set "needs_next_iteration": true if more steps are needed after this one.
Set "needs_salesforce_data": true if you need contact/campaign data from Salesforce.

IMPORTANT RULES:
1. Use placeholders like {{{{Id}}}} for values from previous results
2. NO comments (//) in JSON
3. NO markdown, NO explanations
"""
        
        
        # ✅ NEW: Extract key context for workflow awareness
        original_goal = state.get('user_goal', 'No goal specified')
        task_directive = state.get('task_directive')
        pending_updates = state.get('pending_updates')
        
        # Build workflow context section with clear visual hierarchy
        workflow_context = f"""
===========================================================
WORKFLOW CONTEXT
===========================================================

* ORIGINAL USER GOAL:
{original_goal}
"""
        
        if task_directive:
            workflow_context += f"""
> CURRENT PHASE DIRECTIVE (PRIORITY):
{task_directive}

!  This directive takes precedence. You MUST address this before completing the workflow.
"""
        
        if pending_updates:
            workflow_context += f"""
* PENDING UPDATES:
{json.dumps(pending_updates, indent=2)}

These updates are waiting to be executed. Plan the appropriate tool calls to complete them.
"""
        
        workflow_context += f"""
===========================================================
"""
        
        # Build stage info (previous results context)
        stage_info = ""
        if previous_results:
            # Show more context for reasonable result sets
            # For bulk operations (like email sending), need to see all contacts
            num_results = len(previous_results)
            
            if num_results <= 10:
                # Show all items for small result sets
                stage_info = f"\n\nPREVIOUS RESULTS ({num_results} items):\n{json.dumps(previous_results, indent=2)}"
            else:
                # For large result sets, show summary + sample
                stage_info = f"\n\nPREVIOUS RESULTS: {num_results} items total\n"
                stage_info += f"Sample (first 2):\n{json.dumps(previous_results[:2], indent=2)}\n\n"
                stage_info += "IMPORTANT: ALL items from previous results are available for iteration. "
                stage_info += f"Use 'iterate_over': 'previous_result' to process all {num_results} items."
        else:
            stage_info = "\n\nThis is the FIRST step. No previous results available yet."
        
        # Add available context from state
        context_info = "\n\nAVAILABLE CONTEXT:"
        for context_path in required_context:
            value = _get_nested_value(state, context_path)
            if value:
                # Truncate long values
                value_str = json.dumps(value, indent=2)
                if len(value_str) > 1000:
                    value_str = value_str[:1000] + "..."
                context_info += f"\n- {context_path}: {value_str}"
        
        user_content = f"""{workflow_context}

{stage_info}
{context_info}

PLANNING INSTRUCTIONS:
1. If there is a CURRENT PHASE DIRECTIVE, plan tool calls to fulfill it
2. If there are PENDING UPDATES, plan tool calls to execute them
3. Otherwise, plan the next logical step toward the ORIGINAL USER GOAL
4. Do NOT repeat operations that have already succeeded

Respond with pure JSON (no comments, no markdown)."""
        
        # Call LLM
        llm_model = state.get("planner_model") or "gpt-4o-mini"
        llm_provider = state.get("planner_provider") or "openai"
        
        raw_response = await call_llm(
            system_prompt=system_content,
            user_prompt=user_content,
            default_model=llm_model,
            default_provider=llm_provider,
            default_temperature=0.0,
        )
        
        # Parse response
        raw_text = str(raw_response).strip()
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()
        
        try:
            plan = json.loads(raw_text)
            plan.setdefault("calls", [])
            plan.setdefault("needs_next_iteration", False)
            plan.setdefault("needs_salesforce_data", False)
            return plan
        except json.JSONDecodeError as e:
            logging.error(f"[{service_name}] JSON decode error: {e}")
            return {"calls": [], "needs_next_iteration": False}
    
    # Unknown strategy
    logging.error(f"[{service_name}] Unknown planning strategy: {planning_strategy}")
    return {"calls": [], "needs_next_iteration": False}





def _is_batch_capable_tool(tool_meta: Dict[str, Any], tool_name: str) -> bool:
    """
    Determine if a tool supports batch operations.
    
    Detection strategies:
    1. Tool name contains 'batch' (e.g., send_batch_emails, batch_upsert_salesforce_records)
    2. Tool schema has array parameters like 'recipients' or 'records'
    3. Explicit configuration in MCP metadata
    
    Returns:
        True if the tool supports batch operations, False otherwise
    """
    # Strategy 1: Name-based detection
    if 'batch' in tool_name.lower():
        logging.info(f"✅ [{tool_name}] Detected as batch-capable (name contains 'batch')")
        return True
    
    # Strategy 2: Schema-based detection
    schema = tool_meta.get("schema", {})
    properties = schema.get("properties", {})
    
    # Check for array parameters that indicate batch support
    batch_param_names = ['recipients', 'records', 'items', 'batch_data', 'message_versions']
    for param_name, param_schema in properties.items():
        if param_name in batch_param_names:
            if param_schema.get("type") == "array":
                logging.info(f"✅ [{tool_name}] Detected as batch-capable (has '{param_name}' array parameter)")
                return True
    
    logging.info(f"ℹ️ [{tool_name}] Not batch-capable, will use iteration")
    return False


def _get_batch_parameter_name(tool_meta: Dict[str, Any]) -> Optional[str]:
    """
    Detect the batch parameter name from tool schema.
    
    Returns the name of the array parameter used for batch operations
    (e.g., 'records', 'message_versions', 'recipients', 'items')
    """
    schema = tool_meta.get("schema", {})
    properties = schema.get("properties", {})
    
    # Common batch parameter names in priority order
    batch_param_candidates = ['message_versions', 'records', 'recipients', 'items', 'batch_data']
    
    for param_name in batch_param_candidates:
        if param_name in properties:
            param_schema = properties[param_name]
            if param_schema.get("type") == "array":
                return param_name
    
    return None


def _prepare_batch_arguments(
    arguments: Dict[str, Any],
    batch_records: List[Dict[str, Any]],
    batch_param_name: str,
    tool_name: str
) -> Dict[str, Any]:
    """
    Prepare batch arguments generically based on the batch parameter name.
    
    This replaces hardcoded tool-specific logic with generic schema-based construction.
    """
    # Start with a copy of non-batch arguments
    batch_args = {}
    
    # For each argument, decide if it should be included
    for key, value in arguments.items():
        # Skip the batch parameter itself (we'll add it separately)
        if key == batch_param_name:
            continue
        
        # Skip 'recipients' if we're using 'message_versions' (Brevo specific)
        if batch_param_name == 'message_versions' and key == 'recipients':
            batch_args[key] = []  # Clear recipients when using message_versions
            continue
        
        # For template_id in batch emails, extract from first record and convert to int
        if key == 'template_id' and batch_records:
            template_id_value = batch_records[0].get("template_id", value)
            try:
                batch_args[key] = int(template_id_value)
            except (ValueError, TypeError):
                logging.warning(f"⚠️ Could not convert template_id '{template_id_value}' to int, using as-is")
                batch_args[key] = value
            continue
        
        # Include all other arguments as-is
        batch_args[key] = value
    
    # Add the batch parameter
    batch_args[batch_param_name] = batch_records
    
    return batch_args




def _check_skip_condition(call: Dict[str, Any], result_sets: Dict[str, Any]) -> tuple[bool, str]:
    """Helper to check if a tool should be skipped based on skip_if_exists."""
    arguments = call.get("arguments", {})
    skip_key = call.get("skip_if_exists") or arguments.get("skip_if_exists")
    iterate_over = call.get("iterate_over")
    
    if not skip_key:
        return False, ""
    
    # helper to check object type match
    def is_type_match(key: str) -> bool:
        records = result_sets.get(key)
        if not records or not isinstance(records, list):
            return False
        
        # Get target object name from tool call
        target_obj = arguments.get("object_name") or arguments.get("object")
        if not target_obj:
            return True # If no object name, we can't verify, so assume match (legacy behavior)
            
        # Check first record's type (Salesforce metadata)
        first_rec = records[0]
        if isinstance(first_rec, dict) and "attributes" in first_rec:
            rec_type = first_rec["attributes"].get("type")
            if rec_type and rec_type.lower() != target_obj.lower():
                logging.warning(f"⚠️ Skip logic blocked: Object type mismatch. Tool={target_obj}, Result={rec_type}")
                return False
        return True

    # Safety Check: If we are iterating over this key or a related plural/singular key, 
    # we almost certainly don't want to skip the ENTIRE tool before iteration.
    if iterate_over:
        if iterate_over == skip_key or iterate_over == f"{skip_key}s" or skip_key == f"{iterate_over}s":
            logging.debug(f"⚠️ Not skipping {call.get('tool')} because iterate_over matches skip_if_exists")
            return False, ""

    # Check for direct match
    if result_sets.get(skip_key):
        if is_type_match(skip_key):
            return True, skip_key
    
    # Check for fuzzy match (singular/plural)
    alt_keys = []
    if skip_key.endswith('s'):
        alt_keys.append(skip_key[:-1])
    else:
        alt_keys.append(skip_key + 's')
        
    for alt in alt_keys:
        if result_sets.get(alt):
            if is_type_match(alt):
                return True, alt
            
    return False, ""

async def call_mcp_v2(
    service_name: str,
    config: Dict[str, Any],
    state: MarketingState,
) -> Dict[str, Any]:
    """
    MCP caller (v2) - supports both internal_tool and llm_planner strategies:
    1. Check planning strategy from config
    2. Get plan (via internal tool OR llm_planner)
    3. Execute tools in the plan sequentially
    4. Support iteration for llm_planner strategy
    5. Return results
    """
    server_params = build_mcp_server_params(config)

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize MCP session
            await session.initialize()
            
            # Get available tools for llm_planner strategy
            tools_response = await session.list_tools()
            tools_meta = [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "schema": tool.inputSchema,
                }
                for tool in tools_response.tools
            ]
            
            # Check planning strategy
            planning_strategy = config.get("planning_strategy", "llm_planner")
            logging.info(f"📋 [{service_name}] Planning strategy: {planning_strategy}")
            
            # ✅ CRITICAL: Initialize result_sets with shared_result_sets from state (latest item)
            shared_result_sets = state.get("shared_result_sets", [])
            logging.info(f"📋 [{service_name}]shared_result_sets : {shared_result_sets}")
            # Get latest item from list, or empty dict if list is empty or contains non-dicts
            if shared_result_sets and isinstance(shared_result_sets[-1], dict):
                result_sets = shared_result_sets[-1].copy()
            else:
                result_sets = {}
            
            if result_sets:
                logging.info(f"   🔄 Initialized result_sets from session with keys: {list(result_sets.keys())}")
            
            # Track all tool results and previous results for iteration
            all_tool_results = []
            previous_results = None
            
            # ===== STRATEGY 1: internal_tool (e.g., Salesforce MCP) =====
            if planning_strategy == "internal_tool":
                internal_tool = config.get("planning_tool_name", "generate_all_toolinput")
                
                # ✅ FIX: Explicitly clear ALL shared results for Salesforce to prevent "looping" on old data.
                # User Requirement: "remove everything from shared result set... if it is salesforce mcp"
                # if not state.get("plan_override") and "Salesforce" in service_name:
                #      logging.info(f"🧹 [{service_name}] Wiping SHARED RESULT SETS (User Request) to ensure fresh start")
                #      result_sets.clear()

                # Prepare context for internal tool
                logging.info(f"📋 [{service_name}]shared_result_sets : {result_sets}")
                planning_context = {
                    "user_goal": state.get("user_goal"),
                    "session_context": state.get("session_context", {}),
                    "shared_result_sets": shared_result_sets,  # Pass the list, not the extracted dict
                }
                
                # Check if we have a plan override (resume from interrupt)
                plan = state.get("plan_override") 
                
                if plan:
                     logging.info(f"🔄 [{service_name}] Resuming with overridden plan from user approval (Internal Tool)")
                     calls = plan.get("calls", [])
                else:
                    # Determine intent: if context says "update status", use that
                    # logic is inside generate_all_toolinput or similar
                    
                    # Call internal tool to get plan
                    logging.info(f"📋 [{service_name}] Calling internal tool: {internal_tool}")
                    gen_args = {"query": state.get("user_goal", ""), "context": planning_context}
                    
                    try:
                        gen_result = await session.call_tool(internal_tool, gen_args)
                        logging.info(f"🔍 [gen_result] Raw response: {gen_result}")
                        plan_text = extract_json_response_from_tool_result(gen_result)
                        
                        if not plan_text:
                            logging.warning(f"[{service_name}] Internal tool returned empty plan")
                            return {"execution_summary": {"total_calls": 0}, "tool_results": [], "result_sets": result_sets}
                        
                        plan = json.loads(plan_text)
                        calls = plan.get("calls", [])
                        
                        if not calls:
                            logging.info(f"✅ [{service_name}] No tools to execute")
                            return {"execution_summary": {"total_calls": 0}, "tool_results": [], "result_sets": result_sets}

                        # 🔄 PARTIAL EXECUTION LOOP
                        logging.info(f"🚀 [{service_name}] Starting execution loop ({len(calls)} calls)")
                        tool_results = []
                         # ✅ Track which keys existed before this operation (inherited data)
                        # initial_keys = set(result_sets.keys())
                        keys_added_in_current_operation = set()
                        for idx, call in enumerate(calls):
                             tool_name = call.get("tool", "").lower()
                             
                             # 1. CHECK FOR UNSAFE ACTIONS -> INTERRUPT
                             is_unsafe = any(x in tool_name for x in ["upsert", "delete", "create", "update"])
                             
                             if is_unsafe:
                                 logging.info(f"🛑 [call_mcp_v2] Hit unsafe tool '{tool_name}' - Stopping for PROPOSAL")
                                 remaining_calls = calls[idx:] # Current + Future calls
                                 
                                 # Construct proposal details
                                 args = call.get("arguments", {})
                                 
                                 # Resolve placeholders for UI clarity
                                 try:
                                     resolved_args_for_ui = resolve_tool_placeholders(args, {}, result_sets)
                                 except Exception as e:
                                     logging.warning(f"Failed to resolve args for UI: {e}")
                                     resolved_args_for_ui = args

                                 obj = resolved_args_for_ui.get("object_name") or resolved_args_for_ui.get("object")
                                 
                                 fields = {}
                                 if "records" in resolved_args_for_ui and isinstance(resolved_args_for_ui["records"], list) and resolved_args_for_ui["records"]:
                                      fields = resolved_args_for_ui["records"][0].get("fields", {})
                                 elif "fields" in resolved_args_for_ui:
                                      fields = resolved_args_for_ui["fields"]
                                 else:
                                      fields = resolved_args_for_ui

                                 proposal_details = {
                                     "object_name": obj or "Record",
                                     "fields": fields,
                                     "action_type": "create" if "create" in tool_name or "upsert" in tool_name else "update",
                                     "tool_call": call 
                                 }
                                 
                                #  return {
                                #      "status": "proposal",
                                #      "proposal": proposal_details,
                                #      "generated_plan": {"calls": remaining_calls, "needs_next_iteration": False}, 
                                #      "result_sets": result_sets, # Includes results from safe tools run so far
                                #      "tool_results": tool_results
                                #  }
                                                                   
                                  # ✅ CRITICAL FIX: Only return data fetched in THIS operation (from safe tools)
                                  # Don't include inherited data from previous operations
                                                                    # ✅ CRITICAL FIX: Only return data fetched in THIS operation (from safe tools)
                                  # Don't include inherited data from previous operations
                                 current_operation_data = {}
                                 for key in keys_added_in_current_operation:
                                      if key in result_sets:  # Only include newly added keys
                                          current_operation_data[key] = result_sets[key]
                                
                                 return {
                                      "status": "proposal",
                                      "proposal": proposal_details,
                                      "generated_plan": {"calls": remaining_calls, "needs_next_iteration": False}, 
                                      "result_sets": current_operation_data,  # Only current operation data
                                      "tool_results": tool_results
                                  }

                             # 2. EXECUTE SAFE TOOL
                             logging.info(f"🟢 [call_mcp_v2] Auto-executing safe tool: {tool_name}")
                             arguments = call.get("arguments", {})
                             store_as = call.get("store_as")
                             skip_if_exists = call.get("skip_if_exists")

                             # ✅ CONDITIONAL SKIP: If a result set exists and is non-empty, skip this tool
                             should_skip, reason_key = _check_skip_condition(call, result_sets)
                             if should_skip:
                                 logging.info(f"⏭️ Skipping {tool_name} because '{reason_key}' is already populated")
                                 tool_results.append({
                                     "tool_name": tool_name, 
                                     "status": "skipped", 
                                     "reason": f"Result set '{reason_key}' already exists"
                                 })
                                 continue
                             
                             # ✅ AUTO-FIX: Derive store_as from Query Object if missing
                             if "run_dynamic_soql" in tool_name and not store_as:
                                 query = arguments.get("query", "")
                                 import re
                                 # Simple regex to find "FROM ObjectName"
                                 match = re.search(r"from\s+(\w+)", query, re.IGNORECASE)
                                 if match:
                                     obj_name = match.group(1).lower()
                                     # Handle common pluralization needs if planner expects 'contacts'
                                     # But user asked to use object name.
                                     # If object is 'Contact', store as 'contact'.
                                     # NOTE: If planner expects 'contacts', this might mismatch, but we'll try to rely on fuzzy matching later if needed.
                                     # For now, simplistic derivation.
                                     logging.info(f"   🔧 Auto-setting store_as='{obj_name}' (derived from query)")
                                     store_as = obj_name
                                     
                                     # HACK: If object has 'contact' in it, also alias to 'contacts' via side-channel?
                                     # No, let's stick to user request: Use object name.
                                     if obj_name == 'contact':
                                         store_as = 'contacts' # Planner typically expects plural for iteration
                                 else:
                                     logging.info(f"   🔧 Auto-setting store_as='records' (fallback)")
                                     store_as = "records"
                             
                             try:
                                 resolved_args = resolve_tool_placeholders(arguments, {}, result_sets)
                                 result = await session.call_tool(tool_name, resolved_args)
                                 
                                 # 3. CHECK FOR ERRORS
                                 is_error = getattr(result, 'isError', False)
                                 actual_success = True
                                 error_details = None

                                 # Even if MCP says Success, check if Salesforce content says Failure
                                 if not is_error:
                                     try:
                                         if hasattr(result, 'content') and result.content:
                                             response_text = result.content[0].text if result.content else ""
                                             response_data = json.loads(response_text)
                                             
                                             # Salesforce Error Case 1: Dict with success: False
                                             if isinstance(response_data, dict) and response_data.get('success') == False:
                                                 actual_success = False
                                                 errors = response_data.get('errors', [])
                                                 if errors:
                                                     error_details = errors[0].get('error', 'Unknown error') if isinstance(errors[0], dict) else str(errors[0])
                                                 else:
                                                     error_details = response_data.get('message', 'Operation failed')
                                             
                                             # Salesforce Error Case 2: List of error objects (REST API errors)
                                             elif isinstance(response_data, list) and len(response_data) > 0 and isinstance(response_data[0], dict) and 'errorCode' in response_data[0]:
                                                 actual_success = False
                                                 error_details = response_data[0].get('message', 'Salesforce operation failed')
                                                 logging.error(f"❌ Detected Salesforce List Error: {error_details}")
                                     except Exception as e:
                                         logging.warning(f"Error parsing response for failure check: {e}")
                                         pass

                                 if is_error or not actual_success:
                                     error_msg = error_details or str(result)
                                     logging.error(f"❌ Tool {tool_name} failed: {error_msg}")
                                     
                                     # STOP IMMEDIATELY and return error to the user
                                     return {
                                         "status": "error",
                                         "error": error_msg,
                                         "service": service_name,
                                         "tool_results": tool_results + [{"tool": tool_name, "status": "error", "error": error_msg}],
                                         "result_sets": result_sets,
                                         "execution_summary": {"total_calls": len(tool_results) + 1, "failed_calls": 1}
                                     }
                                 
                                 # 4. HANDLE SUCCESS
                                 logging.info(f"✅ Safe tool {tool_name} succeeded")
                                 tool_results.append({"tool": tool_name, "status": "success", "response": result})
                                 
                                 if store_as:
                                     rows = extract_rows_from_result(result)
                                     if rows:
                                         result_sets[store_as] = rows
                                         keys_added_in_current_operation.add(store_as)
                                         logging.info(f"   💾 Stored {len(rows)} records as '{store_as}'")
                                             
                             except Exception as e:
                                  logging.error(f"❌ Exception running safe tool: {e}")
                                  return {
                                      "status": "error",
                                      "error": str(e),
                                      "service": service_name,
                                      "tool_results": tool_results + [{"tool": tool_name, "status": "error", "error": str(e)}],
                                      "result_sets": result_sets
                                  }
                        
                        # ALL DONE
                        return {
                             "execution_summary": {"total_calls": len(tool_results)}, 
                             "tool_results": tool_results, 
                             "result_sets": result_sets
                        }
                        
                    except Exception as e:
                        logging.error(f"❌ [{service_name}] Failed to get plan: {e}")
                        return {"execution_summary": {"total_calls": 0}, "tool_results": [], "error": str(e), "result_sets": result_sets}
                
                # Execute tools from internal_tool plan (single iteration)
                logging.info(f"🔧 [{service_name}] Executing {len(calls)} tool calls from internal tool plan")
                
            # ===== STRATEGY 2: llm_planner (e.g., Brevo MCP, Linkly MCP) =====
            elif planning_strategy == "llm_planner":
                logging.info(f"🤖 [{service_name}] Using LLM planner strategy with iteration support")
                
                # Iteration loop for llm_planner
                max_iterations = 10
                iteration = 0
                needs_next_iteration = True
                
                while needs_next_iteration and iteration < max_iterations:
                    iteration += 1
                    logging.info(f"\n{'='*60}")
                    logging.info(f"📍 [{service_name}] ITERATION {iteration}/{max_iterations}")
                    logging.info(f"{'='*60}")
                    
                    # Get plan from LLM planner
                    plan = await plan_mcp_execution(
                        service_name=service_name,
                        config=config,
                        tools_meta=tools_meta,
                        state=state,
                        session=session,
                        iteration=iteration,
                        previous_results=previous_results,
                    )
                    
                    calls = plan.get("calls", [])
                    needs_next_iteration = plan.get("needs_next_iteration", False)
                    
                    if not calls:
                        logging.info(f"✅ [{service_name}] No more tools to execute")
                        break
                    
                    logging.info(f"📋 [{service_name}] Plan: {len(calls)} tool calls, needs_next_iteration={needs_next_iteration}")
                    
                    # Execute tools from this iteration's plan
                    iteration_results = []
                    
                    for idx, call in enumerate(calls, start=1):
                        tool_name = call.get("tool")
                        arguments = call.get("arguments", {})
                        store_as = call.get("store_as")
                        iterate_over = call.get("iterate_over")
                        skip_if_exists = call.get("skip_if_exists") or arguments.get("skip_if_exists")
                        
                        if not tool_name:
                            logging.warning(f"⚠️ Skipping call without tool name")
                            continue
                        
                        # ✅ CONDITIONAL SKIP: If a result set exists and is non-empty, skip this tool
                        should_skip, reason_key = _check_skip_condition(call, result_sets)
                        if should_skip:
                            logging.info(f"⏭️ Skipping {tool_name} because '{reason_key}' is already populated")
                            iteration_results.append({
                                "tool_name": tool_name, 
                                "status": "skipped", 
                                "reason": f"Result set '{reason_key}' already exists"
                            })
                            continue
                        
                        logging.info(f"📌 [{service_name}] Executing tool {idx}/{len(calls)}: {tool_name}")
                        
                        # Handle iteration over previous results
                        if iterate_over:
                            iteration_source = result_sets.get(iterate_over, [])
                            
                            if not iteration_source:
                                logging.warning(f"⚠️ iterate_over='{iterate_over}' not found")
                                logging.warning(f"   Available: {list(result_sets.keys())}")
                                iteration_results.append({
                                    "tool_name": tool_name,
                                    "request": arguments,
                                    "error": f"Result set '{iterate_over}' not found",
                                    "status": "error"
                                })
                                continue
                            
                            logging.info(f"🔁 Iterating over {len(iteration_source)} items from '{iterate_over}'")
                            
                            # Execute tool for each item
                            for item_idx, item in enumerate(iteration_source, start=1):
                                resolved_args = resolve_tool_placeholders(arguments, item, result_sets)
                                logging.info(f"   📌 Item {item_idx}/{len(iteration_source)}")
                                
                                try:
                                    result = await session.call_tool(tool_name, resolved_args)
                                    is_error = getattr(result, 'isError', False)
                                    
                                    if is_error:
                                        error_msg = "Unknown error"
                                        if hasattr(result, 'content') and result.content:
                                            for content_item in result.content:
                                                if hasattr(content_item, 'text'):
                                                    error_msg = content_item.text
                                                    break
                                        
                                        logging.error(f"   ❌ Item {item_idx} failed: {error_msg[:200]}")
                                        return {
                                            "status": "error",
                                            "error": error_msg,
                                            "service": service_name,
                                            "tool_results": all_tool_results + iteration_results + [{
                                                "tool_name": tool_name,
                                                "request": resolved_args,
                                                "error": error_msg,
                                                "status": "error"
                                            }],
                                            "result_sets": result_sets
                                        }
                                    else:
                                        # 🔍 Check if Salesforce operation actually succeeded
                                        actual_success = True
                                        error_details = None
                                        
                                        if 'upsert' in tool_name.lower() or 'salesforce' in tool_name.lower():
                                            try:
                                                if hasattr(result, 'content') and result.content:
                                                    response_text = result.content[0].text if result.content else ""
                                                    response_data = json.loads(response_text)
                                                    
                                                    # Case 1: Dict with success: False
                                                    if isinstance(response_data, dict) and response_data.get('success') == False:
                                                        actual_success = False
                                                        errors = response_data.get('errors', [])
                                                        if errors:
                                                            error_details = errors[0].get('error', 'Unknown error') if isinstance(errors[0], dict) else str(errors[0])
                                                        else:
                                                            error_details = response_data.get('message', 'Operation failed')
                                                    
                                                    # Case 2: List of error objects (REST API errors)
                                                    elif isinstance(response_data, list) and len(response_data) > 0 and isinstance(response_data[0], dict) and 'errorCode' in response_data[0]:
                                                        actual_success = False
                                                        error_details = response_data[0].get('message', 'Salesforce operation failed')
                                                        logging.error(f"❌ Detected Salesforce List Error: {error_details}")
                                            except Exception as e:
                                                logging.debug(f"Could not parse response for success check: {e}")
                                                pass
                                        
                                        if not actual_success:
                                            logging.error(f"   ❌ Salesforce operation failed: {error_details}")
                                            error_msg = error_details or "Operation failed"
                                            return {
                                                "status": "error",
                                                "error": error_msg,
                                                "service": service_name,
                                                "tool_results": all_tool_results + iteration_results + [{
                                                    "tool_name": tool_name,
                                                    "request": resolved_args,
                                                    "error": error_msg,
                                                    "status": "error"
                                                }],
                                                "result_sets": result_sets
                                            }
                                        
                                        logging.info(f"   ✅ Item {item_idx} succeeded")
                                        iteration_results.append({
                                            "tool_name": tool_name,
                                            "request": resolved_args,
                                            "response": result,
                                            "status": "success"
                                        })
                                        
                                        # Extract and store results
                                        rows = extract_rows_from_result(result)
                                        if rows:
                                            if not previous_results:
                                                previous_results = []
                                            previous_results.extend(rows)
                                
                                except Exception as e:
                                    logging.error(f"   ❌ Item {item_idx} failed: {e}")
                                    return {
                                        "status": "error",
                                        "error": str(e),
                                        "service": service_name,
                                        "tool_results": all_tool_results + [{"tool": tool_name, "status": "error", "error": str(e)}],
                                        "result_sets": result_sets
                                    }
                                
                                # STOP IMMEDIATELY if any result in iterative execution failed
                                if iteration_results and iteration_results[-1].get("status") == "error":
                                    error_msg = iteration_results[-1].get("error", "Iteration failed")
                                    return {
                                        "status": "error",
                                        "error": error_msg,
                                        "service": service_name,
                                        "tool_results": all_tool_results + iteration_results,
                                        "result_sets": result_sets
                                    }
                        
                        else:
                            # Simple execution (no iteration)
                            try:
                                resolved_arguments = resolve_tool_placeholders(arguments, {}, result_sets)
                                logging.info(f"   🔍 Resolved arguments: {json.dumps(resolved_arguments, indent=2)[:200]}...")
                                
                                result = await session.call_tool(tool_name, resolved_arguments)
                                is_error = getattr(result, 'isError', False)
                                
                                if is_error:
                                    error_msg = "Unknown error"
                                    if hasattr(result, 'content') and result.content:
                                        for item in result.content:
                                            if hasattr(item, 'text'):
                                                error_msg = item.text
                                                break
                                    
                                    logging.error(f"   ❌ Tool failed: {error_msg[:200]}")
                                    return {
                                        "status": "error",
                                        "error": error_msg,
                                        "service": service_name,
                                        "tool_results": all_tool_results + iteration_results + [{
                                            "tool_name": tool_name,
                                            "request": arguments,
                                            "error": error_msg,
                                            "status": "error"
                                        }],
                                        "result_sets": result_sets
                                    }
                                else:
                                    # 🔍 Check if Salesforce operation actually succeeded
                                    actual_success = True
                                    error_details = None
                                    
                                    if 'upsert' in tool_name.lower() or 'salesforce' in tool_name.lower():
                                        try:
                                            if hasattr(result, 'content') and result.content:
                                                response_text = result.content[0].text if result.content else ""
                                                response_data = json.loads(response_text)
                                                
                                                # Case 1: Dict with success: False
                                                if isinstance(response_data, dict) and response_data.get('success') == False:
                                                    actual_success = False
                                                    errors = response_data.get('errors', [])
                                                    if errors:
                                                        error_details = errors[0].get('error', 'Unknown error') if isinstance(errors[0], dict) else str(errors[0])
                                                    else:
                                                        error_details = response_data.get('message', 'Operation failed')
                                                
                                                # Case 2: List of error objects (REST API errors)
                                                elif isinstance(response_data, list) and len(response_data) > 0 and isinstance(response_data[0], dict) and 'errorCode' in response_data[0]:
                                                    actual_success = False
                                                    error_details = response_data[0].get('message', 'Salesforce operation failed')
                                                    logging.error(f"❌ Detected Salesforce List Error: {error_details}")
                                        except Exception as e:
                                            logging.debug(f"Could not parse response for success check: {e}")
                                            pass
                                    
                                    if not actual_success:
                                        logging.error(f"   ❌ Salesforce operation failed: {error_details}")
                                        error_msg = error_details or "Operation failed"
                                        return {
                                            "status": "error",
                                            "error": error_msg,
                                            "service": service_name,
                                            "tool_results": all_tool_results + iteration_results + [{
                                                "tool_name": tool_name,
                                                "request": arguments,
                                                "error": error_msg,
                                                "status": "error"
                                            }],
                                            "result_sets": result_sets
                                        }
                                    
                                    logging.info(f"   ✅ Tool succeeded")
                                    iteration_results.append({
                                        "tool_name": tool_name,
                                        "request": arguments,
                                        "response": result,
                                        "status": "success"
                                    })
                                    
                                    # Store result if requested
                                    if store_as:
                                        rows = extract_rows_from_result(result)
                                        if rows:
                                            result_sets[store_as] = rows
                                            previous_results = rows
                                            logging.info(f"   💾 Stored {len(rows)} records as '{store_as}'")
                            
                            except Exception as e:
                                logging.error(f"   ❌ Tool execution failed: {e}")
                                return {
                                    "status": "error",
                                    "error": str(e),
                                    "service": service_name,
                                    "tool_results": all_tool_results + [{"tool": tool_name, "status": "error", "error": str(e)}],
                                    "result_sets": result_sets
                                }
                            
                            # STOP IMMEDIATELY if the single tool failed
                            if iteration_results and iteration_results[-1].get("status") == "error":
                                error_msg = iteration_results[-1].get("error", "Tool execution failed")
                                return {
                                    "status": "error",
                                    "error": error_msg,
                                    "service": service_name,
                                    "tool_results": all_tool_results + iteration_results,
                                    "result_sets": result_sets
                                }
                    
                    # Add iteration results to all results
                    all_tool_results.extend(iteration_results)
                    
                    # Check if we should continue
                    if not needs_next_iteration:
                        logging.info(f"✅ [{service_name}] Planner indicated completion")
                        break
                
                # Return results from llm_planner iterations
                successful = sum(1 for r in all_tool_results if r.get("status") == "success")
                skipped = sum(1 for r in all_tool_results if r.get("status") == "skipped")
                failed = sum(1 for r in all_tool_results if r.get("status") == "error")
                
                return {
                    "execution_summary": {
                        "total_calls": len(all_tool_results),
                        "successful_calls": successful + skipped,  # Count skipped as success for progress
                        "skipped_calls": skipped,
                        "failed_calls": failed,
                        "iterations": iteration
                    },
                    "tool_results": all_tool_results,
                    "result_sets": result_sets
                }
            
            else:
                logging.error(f"❌ [{service_name}] Unknown planning strategy: {planning_strategy}")
                return {"execution_summary": {"total_calls": 0}, "tool_results": [], "error": f"Unknown planning strategy: {planning_strategy}", "result_sets": result_sets}
            
            # ===== TOOL EXECUTION (for internal_tool strategy) =====
            # Execute tools in the plan
            tool_results = []
            
            for idx, call in enumerate(calls, start=1):
                tool_name = call.get("tool")
                arguments = call.get("arguments", {})
                store_as = call.get("store_as")
                iterate_over = call.get("iterate_over")
                skip_if_exists = call.get("skip_if_exists") or arguments.get("skip_if_exists")
                
                if not tool_name:
                    logging.warning(f"⚠️ Skipping call without tool name")
                    continue
                
                # ✅ CONDITIONAL SKIP: If a result set exists and is non-empty, skip this tool
                should_skip, reason_key = _check_skip_condition(call, result_sets)
                if should_skip:
                    logging.info(f"⏭️ Skipping {tool_name} because '{reason_key}' is already populated")
                    
                    # ✅ PROPAGATE DATA: If store_as is defined, ensure it's populated from the reasoning key
                    # This ensures subsequent tools (like CampaignMember creation) can find the ID
                    if store_as and reason_key and result_sets.get(reason_key):
                        result_sets[store_as] = result_sets[reason_key]
                        logging.info(f"   💾 Mapped '{reason_key}' data to '{store_as}' from skip condition")
                    
                    tool_results.append({
                        "tool_name": tool_name, 
                        "status": "skipped", 
                        "reason": f"Result set '{reason_key}' already exists"
                    })
                    continue
                
                logging.info(f"📌 [{service_name}] Executing tool {idx}/{len(calls)}: {tool_name}")
                
                # Handle iteration
                if iterate_over:
                    # Get iteration source
                    iteration_source = result_sets.get(iterate_over, [])
                    
                    if not iteration_source:
                        logging.warning(f"⚠️ iterate_over='{iterate_over}' not found in result_sets")
                        logging.warning(f"   Available: {list(result_sets.keys())}")
                        tool_results.append({
                            "tool_name": tool_name,
                            "request": arguments,
                            "error": f"Result set '{iterate_over}' not found",
                            "status": "error"
                        })
                        continue
                    
                    logging.info(f"🔁 Iterating over {len(iteration_source)} items from '{iterate_over}'")
                    
                    # ✅ SPECIAL HANDLING: propose_action doesn't use records array
                    if 'propose' in tool_name.lower():
                        logging.info(f"⏭️ Skipping batch for {tool_name} - calling individually")
                        
                        # Call propose_action for each item individually
                        for item_idx, item in enumerate(iteration_source, start=1):
                            resolved_args = resolve_tool_placeholders(arguments, item, result_sets)
                            logging.info(f"   📌 Item {item_idx}/{len(iteration_source)}")
                            logging.info(f"   🔍 Resolved args: {json.dumps(resolved_args, indent=2)}")
                            
                            try:
                                result = await session.call_tool(tool_name, resolved_args)
                                is_error = getattr(result, 'isError', False)
                                
                                if is_error:
                                    error_msg = "Unknown error"
                                    if hasattr(result, 'content') and result.content:
                                        for content_item in result.content:
                                            if hasattr(content_item, 'text'):
                                                error_msg = content_item.text
                                                break
                                    
                                    logging.error(f"   ❌ Item {item_idx} failed: {error_msg[:200]}")
                                    tool_results.append({
                                        "tool_name": tool_name,
                                        "request": resolved_args,
                                        "error": error_msg,
                                        "status": "error"
                                    })
                                else:
                                    logging.info(f"   ✅ Item {item_idx} succeeded")
                                    tool_results.append({
                                        "tool_name": tool_name,
                                        "request": resolved_args,
                                        "response": result,
                                        "status": "success"
                                    })
                                
                                # STOP IMMEDIATELY if any result in iterative execution failed
                                if tool_results and tool_results[-1].get("status") == "error":
                                    error_msg = tool_results[-1].get("error", "Iteration failed")
                                    return {
                                        "status": "error",
                                        "error": error_msg,
                                        "service": service_name,
                                        "tool_results": tool_results,
                                        "result_sets": result_sets
                                    }
                            
                            except Exception as e:
                                logging.error(f"   ❌ Item {item_idx} failed: {e}")
                                error_msg = str(e)
                                return {
                                    "status": "error",
                                    "error": error_msg,
                                    "service": service_name,
                                    "tool_results": tool_results + [{
                                        "tool_name": tool_name,
                                        "request": resolved_args,
                                        "error": error_msg,
                                        "status": "error"
                                    }],
                                    "result_sets": result_sets
                                }
                    
                    else:
                        # ✅ BATCH OTHER TOOLS: upsert_salesforce_records, etc.
                        logging.info(f"🚀 Batching {len(iteration_source)} items into single call")
                    
                    batch_records = []
                    for item in iteration_source:
                        resolved_args = resolve_tool_placeholders(arguments, item, result_sets)
                        
                        # Extract the record from resolved args
                        if 'records' in resolved_args and isinstance(resolved_args['records'], list):
                            batch_records.extend(resolved_args['records'])
                    
                    # Make single batch call
                    batch_args = {
                        "object_name": arguments.get("object_name"),
                        "records": batch_records
                    }
                    
                    logging.info(f"📦 Calling {tool_name} with {len(batch_records)} records")
                    
                    try:
                        result = await session.call_tool(tool_name, batch_args)
                        is_error = getattr(result, 'isError', False)
                        
                        if is_error:
                            error_msg = "Unknown error"
                            if hasattr(result, 'content') and result.content:
                                for content_item in result.content:
                                    if hasattr(content_item, 'text'):
                                        error_msg = content_item.text
                                        break
                            
                            logging.error(f"   ❌ Batch call failed: {error_msg[:200]}")
                            tool_results.append({
                                "tool_name": tool_name,
                                "request": batch_args,
                                "error": error_msg,
                                "status": "error",
                                "batch_size": len(batch_records)
                            })
                        else:
                            logging.info(f"   ✅ Batch call succeeded for {len(batch_records)} records")
                            tool_results.append({
                                "tool_name": tool_name,
                                "request": batch_args,
                                "response": result,
                                "status": "success",
                                "batch_size": len(batch_records)
                            })
                    
                    except Exception as e:
                        logging.error(f"   ❌ Batch call failed: {e}")
                        error_msg = str(e)
                        return {
                            "status": "error",
                            "error": error_msg,
                            "service": service_name,
                            "tool_results": tool_results + [{
                                "tool_name": tool_name,
                                "request": batch_args,
                                "error": error_msg,
                                "status": "error",
                                "batch_size": len(batch_records)
                            }],
                            "result_sets": result_sets
                        }
                    
                    # STOP IMMEDIATELY if batch call failed
                    if tool_results and tool_results[-1].get("status") == "error":
                         error_msg = tool_results[-1].get("error", "Batch operation failed")
                         return {
                             "status": "error",
                             "error": error_msg,
                             "service": service_name,
                             "tool_results": tool_results,
                             "result_sets": result_sets
                         }
                
                else:
                    # Simple execution (no iteration)
                    try:
                        # 🔥 CRITICAL: Resolve placeholders in arguments BEFORE calling tool
                        # This ensures {{campaign.Id}} uses the LATEST campaign from result_sets
                        resolved_arguments = resolve_tool_placeholders(arguments, {}, result_sets)
                        logging.info(f"   🔍 Resolved arguments: {json.dumps(resolved_arguments, indent=2)[:200]}...")
                        
                        result = await session.call_tool(tool_name, resolved_arguments)
                        
                        # Check if error
                        is_error = getattr(result, 'isError', False)
                        
                        if is_error:
                            error_msg = "Unknown error"
                            if hasattr(result, 'content') and result.content:
                                for item in result.content:
                                    if hasattr(item, 'text'):
                                        error_msg = item.text
                                        break
                            
                            logging.error(f"   ❌ Tool failed: {error_msg[:200]}")
                            tool_results.append({
                                "tool_name": tool_name,
                                "request": arguments,
                                "error": error_msg,
                                "status": "error"
                            })
                        else:
                            # 🔍 Check if Salesforce operation actually succeeded
                            # Even if isError=False, the Salesforce API might return success:false
                            actual_success = True
                            error_details = None
                            
                            if 'upsert' in tool_name.lower() or 'salesforce' in tool_name.lower():
                                try:
                                    if hasattr(result, 'content') and result.content:
                                        response_text = result.content[0].text if result.content else ""
                                        response_data = json.loads(response_text)
                                        
                                        # Case 1: Dict with success: False
                                        if isinstance(response_data, dict) and response_data.get('success') == False:
                                            actual_success = False
                                            errors = response_data.get('errors', [])
                                            if errors:
                                                error_details = errors[0].get('error', 'Unknown error') if isinstance(errors[0], dict) else str(errors[0])
                                            else:
                                                error_details = response_data.get('message', 'Operation failed')
                                        
                                        # Case 2: List of error objects (REST API errors)
                                        elif isinstance(response_data, list) and len(response_data) > 0 and isinstance(response_data[0], dict) and 'errorCode' in response_data[0]:
                                            actual_success = False
                                            error_details = response_data[0].get('message', 'Salesforce operation failed')
                                            logging.error(f"❌ Detected Salesforce List Error: {error_details}")
                                except Exception as e:
                                    logging.debug(f"Could not parse response for success check: {e}")
                                    pass
                            
                            if not actual_success:
                                logging.error(f"   ❌ Salesforce operation failed: {error_details}")
                                error_msg = error_details or "Operation failed"
                                return {
                                    "status": "error",
                                    "error": error_msg,
                                    "service": service_name,
                                    "tool_results": tool_results + [{
                                        "tool_name": tool_name,
                                        "request": arguments,
                                        "error": error_msg,
                                        "status": "error"
                                    }],
                                    "result_sets": result_sets
                                }
                            else:
                                logging.info(f"   ✅ Tool succeeded")
                                tool_results.append({
                                    "tool_name": tool_name,
                                    "request": arguments,
                                    "response": result,
                                    "status": "success"
                                })
                            
                            # Store result if requested
                            if store_as:
                                rows = extract_rows_from_result(result)
                                if rows:
                                    result_sets[store_as] = rows
                                    logging.info(f"   💾 Stored {len(rows)} records as '{store_as}'")
                            
                            # ✅ AUTO-STORE: Automatically store upsert results by object_name
                            # This enables placeholder resolution ({{campaign.Id}}) and session persistence
                            if 'upsert' in tool_name.lower():
                                # Extract object_name from arguments
                                object_name = arguments.get('object_name') or arguments.get('object')
                                
                                if object_name:
                                    logging.info(f"   🔍 Auto-storage: Checking upsert result for {object_name}")
                                    
                                    try:
                                        response_data = None
                                        
                                        # Extract response data
                                        if hasattr(result, 'content') and result.content:
                                            response_text = result.content[0].text if result.content else ""
                                            response_data = json.loads(response_text)
                                        elif isinstance(result, dict):
                                            response_data = result
                                        
                                        if response_data:
                                            logging.info(f"   🔍 Response data: success={response_data.get('success')}, results={len(response_data.get('results', []))}")
                                            
                                            # Check if successful
                                            if response_data.get('success') and response_data.get('results'):
                                                stored_records = []
                                                
                                                # Extract all successful records
                                                for idx, res in enumerate(response_data['results']):
                                                    if res.get('success') and res.get('record_id'):
                                                        # Create record with Id field
                                                        stored_record = {'Id': res['record_id']}
                                                        
                                                        # Add fields from the request
                                                        if 'records' in arguments and arguments['records']:
                                                            # Get corresponding record from request
                                                            if idx < len(arguments['records']):
                                                                request_fields = arguments['records'][idx].get('fields', {})
                                                                stored_record.update(request_fields)
                                                        
                                                        stored_records.append(stored_record)
                                                
                                                if stored_records:
                                                    # Store using lowercase object name as key
                                                    # This REPLACES any old data for this object
                                                    store_key = object_name.lower()
                                                    result_sets[store_key] = stored_records
                                                    logging.info(f"   💾 Auto-stored {len(stored_records)} {object_name} record(s) as '{store_key}' (replaced old data)")
                                                    logging.info(f"   📋 First record: {json.dumps(stored_records[0], indent=2)}")
                                                else:
                                                    logging.warning(f"   ⚠️ No successful records to auto-store for {object_name}")
                                            else:
                                                logging.warning(f"   ⚠️ Upsert failed or returned no results for {object_name}")
                                        else:
                                            logging.warning(f"   ⚠️ Could not extract response data for auto-storage")
                                    
                                    except Exception as e:
                                        logging.error(f"   ❌ Auto-storage error: {e}", exc_info=True)
                                else:
                                    logging.warning(f"   ⚠️ No object_name found in arguments for auto-storage")
                    
                    except Exception as e:
                        logging.error(f"   ❌ Tool execution failed: {e}")
                        error_msg = str(e)
                        return {
                            "status": "error",
                            "error": error_msg,
                            "service": service_name,
                            "tool_results": tool_results + [{
                                "tool_name": tool_name,
                                "request": arguments,
                                "error": error_msg,
                                "status": "error"
                            }],
                            "result_sets": result_sets
                        }
            
            # Build execution summary
            successful = sum(1 for r in tool_results if r.get("status") == "success")
            skipped = sum(1 for r in tool_results if r.get("status") == "skipped")
            failed = sum(1 for r in tool_results if r.get("status") == "error")
            
            return {
                "execution_summary": {
                    "total_calls": len(tool_results),
                    "successful_calls": successful + skipped, # Count skipped as success for progress
                    "skipped_calls": skipped,
                    "failed_calls": failed
                },
                "tool_results": tool_results,
                "result_sets": result_sets
            }

 

def extract_rows_from_result(result) -> Optional[List[Dict[str, Any]]]:
    """
    Extract record rows from MCP tool result.
    Handles various response formats from different MCP servers.
    """
    try:
        logging.info(f"[extract_rows] result: {result!r}")
        # 1) Handle MCP result with content attribute (common pattern)
        if hasattr(result, 'content'):
            for item in result.content:
                if hasattr(item, 'text'):
                    raw = item.text
                    logging.debug(f"[extract_rows] raw text: {raw!r}")
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        logging.debug("[extract_rows] Not JSON, skipping this content item")
                        continue

                    # ---- CASE A: SOQL-style dict with 'records' ----
                    if isinstance(data, dict):
                        # Salesforce query style: {"records": [...], "total": N}
                        if 'records' in data and isinstance(data['records'], list):
                            records = data['records']
                            logging.debug(f"[extract_rows] Found {len(records)} records in 'records'")
                            return records or None

                        # Generic result wrappers
                        if 'result' in data:
                            if isinstance(data['result'], list):
                                logging.debug(f"[extract_rows] Found {len(data['result'])} records in 'result'")
                                return data['result'] or None
                            if isinstance(data['result'], dict) and 'records' in data['result']:
                                records = data['result']['records']
                                logging.debug(f"[extract_rows] Found {len(records)} in 'result.records'")
                                return records or None

                        if 'data' in data and isinstance(data['data'], list):
                            logging.debug(f"[extract_rows] Found {len(data['data'])} records in 'data'")
                            return data['data'] or None

                        # ---- CASE B: single create/update result with id ----
                        # e.g. {"success": true, "id": "003..."}
                        if 'id' in data and 'records' not in data:
                            rec: Dict[str, Any] = dict(data)
                            # normalise Id so {{Id}} works in the planner
                            rec['Id'] = rec.get('Id') or rec['id']
                            logging.debug(f"[extract_rows] Normalized single record with Id={rec['Id']}")
                            return [rec]

                    # ---- CASE C: direct list ----
                    if isinstance(data, list) and data:
                        logging.debug(f"[extract_rows] Found {len(data)} records as direct list")
                        return data

        # 2) Handle structuredContent attribute (fallback)
        if hasattr(result, 'structuredContent'):
            structured = result.structuredContent
            logging.debug(f"[extract_rows] structuredContent: {structured!r}")

            if isinstance(structured, dict):
                for key in ['records', 'result', 'data', 'rows']:
                    if key in structured and isinstance(structured[key], list):
                        records = structured[key]
                        if records:
                            logging.debug(f"[extract_rows] Found {len(records)} records in structuredContent['{key}']")
                            return records

        logging.debug("[extract_rows] No records found in result")
        return None

    except Exception as e:
        logging.debug(f"[extract_rows] Could not extract rows from result: {e}")
        return None

def resolve_tool_placeholders(
    arguments: Dict[str, Any], 
    record: Dict[str, Any],
    result_sets: Optional[Dict[str, List[Dict[str, Any]]]] = None
) -> Dict[str, Any]:
    """Recursively replace {{field}} placeholders in arguments with values from record or result_sets.
    
    Supports:
    - {{Id}} - from current iteration record
    - {{campaign.Id}} - from named result set 'campaign'
    """
    result_sets = result_sets or {}
    logging.info(f"🔍 [resolve_placeholders] Available result_sets keys: {list(result_sets.keys())}")
    
    def replace_value(value, is_sql_context=False):
        if isinstance(value, str):
            # Check if this looks like a SQL query
            is_sql = any(keyword in value.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE'])
            
            def replacer(match):
                full_match = match.group(1).strip()
                logging.info(f"🔍 [replacer] Found match: '{full_match}'")
                
                # Check for dotted notation: result_name.field
                if '.' in full_match:
                    result_name, field_name = full_match.split('.', 1)
                    
                    # Case-insensitive lookup for result_name
                    # Find the actual key in result_sets that matches result_name (ignoring case)
                    actual_result_key = next((k for k in result_sets.keys() if k.lower() == result_name.lower()), None)
                    
                    if actual_result_key:
                        # ✅ Check if result set has items before accessing index 0
                        items = result_sets[actual_result_key]
                        if not items:
                            logging.info(f"🔍 [resolve_placeholders] Result set '{actual_result_key}' is empty, using empty string for placeholder")
                            return ""
                            
                        # Get first item from named result set
                        named_record = items[0]
                        if field_name in named_record:
                            replacement = named_record[field_name]
                            
                            # 🧹 CLEANING HEURISTIC: Fix "3 - Name" format for IDs
                            # If value looks like "Integer - String", extract the integer.
                            if isinstance(replacement, str) and " - " in replacement:
                                import re
                                match_id = re.match(r'^(\d+)\s+-\s+.*', replacement)
                                if match_id:
                                    cleaned_id = match_id.group(1)
                                    logging.info(f"🧹 Cleaned '{replacement}' to '{cleaned_id}'")
                                    replacement = int(cleaned_id) # Return as int to satisfy pydantic if possible, or str info
                                    # Pydantic will accept int for int field. If we return str, it tries to parse.
                                    # But `resolve_tool_placeholders` returns STR usually for regex replacement.
                                    # Wait, `re.sub` expects string return.
                                    # If we return int, `re.sub` will crash?
                                    # YES. replacer MUST return string.
                                    replacement = cleaned_id

                            logging.info(f"Replacing {{{{result_name}}.{{field_name}}}}: {replacement}")
                            
                            # ✅ FIX: Don't add quotes if the placeholder is already surrounded by quotes
                            # Check the original string to see if {{placeholder}} is already quoted
                            if is_sql and isinstance(replacement, str):
                                # Check if the match is already within quotes by looking at the context
                                # The pattern '{{...}}' might already be within '{{...}}'
                                # We'll just return the replacement without quotes since the SQL likely has them
                                return str(replacement)
                            return str(replacement)
                        else:
                            logging.warning(f"⚠️ Field '{field_name}' not found in result set '{actual_result_key}'")
                            return match.group(0)
                    else:
                        logging.warning(f"⚠️ Result set '{result_name}' (normalized: {result_name.lower()}) not found. Available: {list(result_sets.keys())}")
                        return match.group(0)
                
                # Fall back to current iteration record
                field_name = full_match
                logging.info(f"Replacing placeholder: {field_name} with value: {record.get(field_name)}")
                if field_name in record:
                    replacement = record[field_name]
                    # Only add quotes if it's a SQL context AND the value is a string
                    if is_sql and isinstance(replacement, str):
                        return f"'{replacement}'"
                    return str(replacement)
                else:
                    logging.warning(f"⚠️ Placeholder {field_name} not found in record: {record}")
                    return match.group(0)
            
            return re.sub(r'\{\{([^}]+)\}\}', replacer, value)
        
        elif isinstance(value, dict):
            return {k: replace_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [replace_value(item) for item in value]
        else:
            return value
    
    return replace_value(arguments)

def extract_json_response_from_tool_result(result) -> Optional[str]:
    """
    Extract json_response field from Salesforce MCP generate_all_toolinput result.
    """
    try:
        # Check structuredContent first
        if hasattr(result, 'structuredContent'):
            structured = result.structuredContent
            if isinstance(structured, dict):
                if 'result' in structured and isinstance(structured['result'], dict):
                    if 'json_response' in structured['result']:
                        return structured['result']['json_response']
        
        # Check content attribute
        if hasattr(result, 'content'):
            for item in result.content:
                if hasattr(item, 'text'):
                    try:
                        data = json.loads(item.text)
                        if isinstance(data, dict) and 'json_response' in data:
                            return data['json_response']
                    except json.JSONDecodeError:
                        continue
        
        logging.error("Could not find json_response in tool result")
        return None
        
    except Exception as e:
        logging.error(f"Error extracting json_response: {e}")
        return None

async def execute_single_tool(
    service_name: str,
    tool_name: str,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Executes a SINGLE MCP tool directly without any planning logic.
    Useful for deterministic workflows (LangGraph nodes).
    """

    # Get config
    registry = get_member_dependency("Marketing Agent")
    config = registry.get(service_name)
    if not config:
        raise ValueError(f"Service {service_name} not found")

    server_params = build_mcp_server_params(config)

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            logging.info(f"🔧 [execute_single_tool] Calling {service_name}.{tool_name}")
            logging.info(f"   Args: {json.dumps(arguments, indent=2)[:500]}...")

            try:
                result = await session.call_tool(tool_name, arguments)
                
                # Check for error
                is_error = getattr(result, 'isError', False)
                if is_error:
                    error_msg = "Unknown error"
                    if hasattr(result, 'content'):
                        for item in result.content:
                            if hasattr(item, 'text'):
                                error_msg = item.text
                                break
                    logging.error(f"❌ [execute_single_tool] Failed: {error_msg}")
                    return {"status": "error", "error": error_msg}
                
                # Extract content
                content_text = ""
                if hasattr(result, 'content'):
                    for item in result.content:
                        if hasattr(item, 'text'):
                            content_text += item.text
                
                # Try parsing JSON
                try:
                    parsed = json.loads(content_text)
                    return {"status": "success", "data": parsed, "raw": content_text}
                except:
                    return {"status": "success", "data": content_text, "raw": content_text}

            except Exception as e:
                logging.error(f"❌ [execute_single_tool] Exception: {e}")
                return {"status": "error", "error": str(e)}

# ------------------------------------------------------------------------------
# MCP Tool Pre-loading (Moved from core/mcp_loader.py)
# ------------------------------------------------------------------------------

# Global cache for preloaded tools
_PRELOADED_TOOLS: Dict[str, List[Dict[str, Any]]] = {}

async def preload_mcp_tools(service_configs: Dict[str, Dict[str, Any]]):
    """
    Pre-load all MCP tools at application startup.
    Call this once when the FastAPI server starts.
    """
    logging.info("🚀 Pre-loading MCP tools...")
    
    for service_name, config in service_configs.items():
        try:
            logging.info(f"⏳ Fetching tools for {service_name}...")
            
            # Use strict server params construction to avoid conflict with existing build_mcp_server_params
            server_params = StdioServerParameters(
                command=config["command"],
                args=config["args"],
                env=config.get("env")
            )
            
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools_response = await session.list_tools()
                    
                    tools_meta = []
                    for tool in tools_response.tools:
                        tools_meta.append({
                            "name": tool.name,
                            "description": getattr(tool, "description", ""),
                            "schema": getattr(tool, "inputSchema", {}),
                        })
                    
                    _PRELOADED_TOOLS[service_name] = tools_meta
                    logging.info(f"✅ Preloaded {len(tools_meta)} tools for {service_name}")
                    
        except Exception as e:
            logging.error(f"❌ Failed to preload tools for {service_name}: {e}")
    
    logging.info(f"🎉 Preloaded tools for {len(_PRELOADED_TOOLS)} services: {list(_PRELOADED_TOOLS.keys())}")

def get_preloaded_tools(service_name: str) -> List[Dict[str, Any]]:
    """Get preloaded tools for a service."""
    return _PRELOADED_TOOLS.get(service_name, [])
