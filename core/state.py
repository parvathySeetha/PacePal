# core/state.py
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
import operator


def merge_dicts(left: Optional[Dict], right: Optional[Dict]) -> Optional[Dict]:
    """
    Reducer that merges dicts, preserving left if right is None/empty.
    This ensures salesforce_data persists across state updates.
    """
     
    if right is None or (isinstance(right, dict) and len(right) == 0):
        return left
    if left is None:
        return right
    # Merge: right takes precedence for overlapping keys
    result = {**left, **right}
    return result



def merge_history(left: Optional[List[Dict]], right: Optional[List[Dict]]) -> Optional[List[Dict]]:
    """
    Reducer that appends new history items to the existing list.
    """
    if left is None:
        return right
    if right is None:
        return left
    return left + right





