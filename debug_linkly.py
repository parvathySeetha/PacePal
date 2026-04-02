import asyncio
import sys
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Add project root AND linklymcp root to sys.path
project_root = Path("/Users/ajaydas/Desktop/Multi agent /Pacepal_Agent")
linkly_root = project_root / "mcp_module" / "linklymcp"

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(linkly_root)) # This allows 'from Error', 'from Client', 'from config' to work

try:
    # Now we can import directly as the code expects
    from Client.Linkly_client import LinklyApiClient
    from tools.track_link_clicks import track_link_clicks
    logging.info("✅ Successfully imported Linkly modules")
except ImportError as e:
    logging.error(f"❌ ImportError: {e}")
    # Try alternate import if the above fails (in case namespace packages behave differently)
    try: 
        from mcp_module.linklymcp.Client.Linkly_client import LinklyApiClient
        from mcp_module.linklymcp.tools.track_link_clicks import track_link_clicks
        logging.info("✅ Successfully imported Linkly modules (alternate path)")
    except ImportError as e2:
        logging.error(f"❌ Alternate ImportError: {e2}")
        sys.exit(1)


async def test_client():
    logging.info("\n--- Testing Client Directly ---")
    try:
        client = LinklyApiClient()
        # IDs from user log
        link_ids = [38402522, 38402524, 38402528]
        
        logging.info(f"Using Workspace ID: {client.workspace_id}")
        
        for link_id in link_ids:
            logging.info(f"Testing Link ID: {link_id}")
            endpoint = f"/api/v1/workspace/{client.workspace_id}/clicks"
            params = {
                "link_id": str(link_id),
                "unique": "true",
                "format": "json",
                "frequency": "day",
                "start": "2026-01-20", # Approximate range from user logs
                "end": "2026-02-19",
                "bots": "false"
            }
            
            try:
                # Use our new params method!
                res = await client.request(endpoint, params=params)
                logging.info(f"Result for {link_id}: {res}")
            except Exception as e:
                logging.error(f"Error for {link_id}: {e}")
                
        await client.close()
    except Exception as e:
        import traceback
        logging.error(f"Client test failed: {e}")
        logging.error(traceback.format_exc())

async def test_tool():
    logging.info("\n--- Testing Tool Directly ---")
    
    link_ids = [38402522, 38402524, 38402528]
    
    try:
        res = await track_link_clicks(
            link_ids=link_ids, 
            start_date="2026-01-20",
            end_date="2026-02-19",
            debug=True
        )
        logging.info(f"Tool Result Status: {res.get('status')}")
        logging.info(f"Tool Result Clicks Per Link: {res.get('clicks_per_link')}")
        
        if res.get('status') == 'success':
             logging.info(f"Tool Result Full: {res}")
        else:
             logging.error(f"❌ Tool Failed. Full Result: {res}")
             
    except Exception as e:
        import traceback
        logging.error(f"Tool Error: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    # Run tests
    asyncio.run(test_client())
    asyncio.run(test_tool())
