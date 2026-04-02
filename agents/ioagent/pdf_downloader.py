import asyncio
import os
import requests
import json
import sys
import logging
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
IOAGENT_DIR = Path(__file__).resolve().parent

#SERVER_SCRIPT_PATH = PROJECT_ROOT / "mcp_module" / "Salesforcemcp" / "sf_server.py"

SERVER_SCRIPT_PATH = PROJECT_ROOT / "mcp_module" / "Salesforcemcp" / "sf_server.py"

async def get_mcp_session_info(session):
    """Helper to get session info from MCP"""
    session_info = await session.call_tool("get_session_info", arguments={})
    if not session_info.content:
        raise Exception("Failed to retrieve session info (empty response).")
    
    session_data = json.loads(session_info.content[0].text)
    if "error" in session_data:
        raise Exception(f"Error getting session info: {session_data['error']}")
        
    return session_data

async def get_case_attachments_async(case_id: str):
    """
    Lists all attachments (ContentDocumentLinks) for a given Case ID.
    """
    server_path = SERVER_SCRIPT_PATH
    server_params = StdioServerParameters(command=sys.executable, args=[str(server_path)])

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Query for attachments
                # Query for attachments
                query = f"""
                    SELECT ContentDocumentId, ContentDocument.Title, ContentDocument.FileExtension, SystemModstamp 
                    FROM ContentDocumentLink 
                    WHERE LinkedEntityId = '{case_id}' 
                    ORDER BY SystemModstamp DESC
                """
            
                result = await session.call_tool("run_dynamic_soql", arguments={"query": query})
                
                records = []
                if result.content:
                    try:
                        data = json.loads(result.content[0].text)
                        records = data.get('records', [])
                    except json.JSONDecodeError:
                        records = []

                if not records:
                    # Fallback to EmailMessage
                    query1=f"""SELECT Id
                                FROM EmailMessage
                                WHERE ParentId = '{case_id}'
                                ORDER BY CreatedDate DESC limit 1"""

                    result1 = await session.call_tool("run_dynamic_soql", arguments={"query": query1})
                    logger.info(f"Email Message Query Result: {result1}")
                    if result1.content:
                        try:
                            data1 = json.loads(result1.content[0].text)
                            records1 = data1.get('records', [])
                            
                            if records1:
                                email_id = records1[0]['Id']

                                query2 = f"""
                                SELECT ContentDocumentId, ContentDocument.Title, ContentDocument.FileExtension, SystemModstamp 
                                FROM ContentDocumentLink 
                                WHERE LinkedEntityId = '{email_id}' 
                                ORDER BY SystemModstamp DESC
                                """
                                result2 = await session.call_tool("run_dynamic_soql", arguments={"query": query2})
                                if result2.content:
                                    data2 = json.loads(result2.content[0].text)
                                    records = data2.get('records', [])
                        except Exception:
                            # If fallback fails, return empty list
                            pass
            
            return records
    except Exception as e:
        print(f"Error listing attachments: {e}")
        return []

async def download_specific_attachment_async(content_document_id: str, save_directory: str):
    """
    Downloads a specific ContentDocument by ID.
    """
    server_path = SERVER_SCRIPT_PATH
    server_params = StdioServerParameters(command=sys.executable, args=[str(server_path)])

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Get Session Info for Download URL
                session_data = await get_mcp_session_info(session)
                session_id = session_data.get("session_id")
                instance_url = session_data.get("instance_url")

                # Query Version Data
                query = f"""
                    SELECT Title, FileExtension, VersionData 
                    FROM ContentVersion 
                    WHERE ContentDocumentId = '{content_document_id}' 
                    AND IsLatest = true
                """
                
                result = await session.call_tool("run_dynamic_soql", arguments={"query": query})
                if not result.content:
                    raise Exception("Error querying ContentVersion.")

                version_data = json.loads(result.content[0].text)
                if not version_data.get('records'):
                    raise Exception("No ContentVersion found.")
                    
                record = version_data['records'][0]
                file_name = f"{record['Title']}.{record['FileExtension']}"
                download_path = record['VersionData']
                full_url = f"https://{instance_url}{download_path}"

                # Download
                headers = {
                    "Authorization": "Bearer " + session_id,
                    "Content-Type": "application/octet-stream"
                }

                response = requests.get(full_url, headers=headers, stream=True)
                response.raise_for_status()
                
                os.makedirs(save_directory, exist_ok=True)
                full_save_path = os.path.join(save_directory, file_name)

                with open(full_save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        
                return full_save_path

    except Exception as e:
        print(f"Error downloading attachment: {e}")
        return None

# Synchronous Wrappers
def get_case_attachments(case_id: str):
    logger.info(f"Getting attachments for Case ID in PDF_downloader: {case_id}")
    return asyncio.run(get_case_attachments_async(case_id))

def download_attachment(content_document_id: str, save_directory: str):
    return asyncio.run(download_specific_attachment_async(content_document_id, save_directory))

# Legacy wrapper for backward compatibility if needed, or remove
def download_case_attachment(case_id: str):
    # Default behavior: download latest
    attachments = get_case_attachments(case_id)
    if not attachments:
        return "Error: No attachments found."
    
    latest = attachments[0]
    download_dir = PROJECT_ROOT / "agents/ioagent/downloads"
    path = download_attachment(latest['ContentDocumentId'], str(download_dir))
    if path:
        return f"Success: File downloaded to {path}"
    return "Error: Download failed."

if __name__ == "__main__":
    # Test
    cid = "500f6000007HSbmAAG"
    atts = get_case_attachments(cid)
    print(f"Attachments: {atts}")
    if atts:
        p = download_attachment(atts[0]['ContentDocumentId'], "downloads")
        print(f"Downloaded to: {p}")