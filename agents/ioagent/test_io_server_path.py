
import sys
from pathlib import Path

# Mock FastAPI for a small test
current_file = Path(__file__).resolve()
ioagent_dir = current_file.parent
project_root = ioagent_dir.parent.parent

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

print(f"Testing import from mcp_module.Salesforcemcp.get_prompts")
try:
    from mcp_module.Salesforcemcp.get_prompts import fetch_prompts
    print("✅ Import successful")
    prompts = fetch_prompts()
    print(f"✅ fetch_prompts() called. Found {len(prompts)} prompts.")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
