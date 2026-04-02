from typing import Optional, TypedDict, List, Dict, Any, Annotated
from agents.marketing.state import MarketingState
from langchain_core.messages import BaseMessage

# Define PacepalState State (extends MarketingState context)
class PacepalState(MarketingState):
    selected_agent: Optional[str]
    record_id: Optional[str]
