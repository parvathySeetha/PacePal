"""Configuration for deployment modes and agent URLs"""
import os
from typing import Literal


# Deployment mode: "monolith" or "distributed"
DEPLOYMENT_MODE: Literal["monolith", "distributed"] = os.getenv(
    "DEPLOYMENT_MODE", "monolith"
)

# Agent URLs - "local" means use in-process subgraph
AGENT_URLS = {
    "marketing": os.getenv("MARKETING_AGENT_URL", "local"),
    "integration": os.getenv("INTEGRATION_AGENT_URL", "local"),
    "reconcillation": os.getenv("RECONCILLATION_AGENT_URL", "local"),
    "io": os.getenv("IO_AGENT_URL", "local"),
}

# Server ports
PACEPAL_PORT = int(os.getenv("PACEPAL_PORT", "8001"))
MARKETING_PORT = int(os.getenv("MARKETING_PORT", "8002"))
RECONCILLATION_PORT = int(os.getenv("RECONCILLATION_PORT", "8003"))
#INTEGRATION_PORT = int(os.getenv("INTEGRATION_PORT", "8003")) # Alias/deprecated?
IO_PORT = int(os.getenv("IO_PORT", "8004"))

# Timeouts
AGENT_TIMEOUT = float(os.getenv("AGENT_TIMEOUT", "300.0"))  # 5 minutes default
