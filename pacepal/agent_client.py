"""HTTP client for communicating with remote agents"""
import httpx
import logging
from typing import Dict, Any
from shared.models import AgentRequest, AgentResponse
from shared.config import AGENT_URLS, AGENT_TIMEOUT

logger = logging.getLogger(__name__)


class AgentClient:
    """HTTP client for communicating with remote agents"""
    
    def __init__(self, timeout: float = None):
        self.timeout = timeout or AGENT_TIMEOUT
        
    async def call_agent(
        self, 
        agent_name: str, 
        request: AgentRequest,
        agent_url: str = None
    ) -> AgentResponse:
        """
        Call a remote agent via HTTP.
        Raises exception if agent is unreachable.
        
        Args:
            agent_name: Name of the agent (e.g., "marketing", "integration")
            request: AgentRequest object with task details
            agent_url: Optional explicit URL to use (bypasses config)
            
        Returns:
            AgentResponse from the remote agent
            
        Raises:
            ValueError: If agent is not configured for remote access
            httpx.TimeoutException: If request times out
            httpx.HTTPError: If HTTP error occurs
        """
        url = agent_url or AGENT_URLS.get(agent_name)
        endpoint=url
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"🌐 Calling remote agent: {agent_name} at {endpoint}")
                logger.debug(f"   Request: {request.user_goal[:100]}...")
                
                response = await client.post(
                    endpoint,
                    json=request.model_dump()
                )
                response.raise_for_status()
                
                agent_response = AgentResponse(**response.json())
                logger.info(f"✅ Remote agent {agent_name} responded: {agent_response.status}")
                
                return agent_response
                
        except httpx.TimeoutException:
            logger.error(f"❌ Timeout calling {agent_name} (timeout: {self.timeout}s)")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HTTP {e.response.status_code} error calling {agent_name}: {e}")
            raise
        except httpx.HTTPError as e:
            logger.error(f"❌ HTTP error calling {agent_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error calling {agent_name}: {e}")
            raise
    
    async def health_check(self, agent_name: str) -> bool:
        """
        Check if an agent is healthy and reachable.
        
        Returns:
            True if agent is healthy, False otherwise
        """
        url = AGENT_URLS.get(agent_name)
        
        if not url or url == "local":
            return True  # Local agents are always "healthy"
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{url}/health")
                return response.status_code == 200
        except Exception as e:
            logger.warning(f"⚠️ Health check failed for {agent_name}: {e}")
            return False
