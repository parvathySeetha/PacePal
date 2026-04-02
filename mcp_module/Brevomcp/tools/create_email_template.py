from __future__ import annotations
from typing import Dict, Any, Optional
import json
import traceback
from client.brevo_client import BrevoApiClient
from Error.brevo_error import BrevoApiError


async def create_email_template(
    template_name: str,
    subject: str,
    html_content: str,
    sender_name: str = "Aleena Mathews",
    sender_email: Optional[str] = None
) -> str:
    """
    Creates a new transactional email template in Brevo using the BrevoApiClient.
    
    Args:
        template_name (str): Name of the template in Brevo.
        subject (str): Subject line.
        html_content (str): The HTML body.
        sender_name (str): Name of sender.
        sender_email (str): Optional sender email. If not provided, relies on defaults or env vars.

    Returns:
        str: JSON string with "id" of created template or error.
    """
    
    # Determine sender email (logic moved from direct env access to variable check, 
    # client handles basic config but we need to pass a valid email to the API)
    # The client config doesn't expose SENDER_EMAIL directly, so we'll check logic here or let user pass it.
    # For robust tool usage, we can default to a known email if None.
    final_sender_email = sender_email or "aleenamathews2001@gmail.com" # Default fallback similar to send_batch_emails

    client = BrevoApiClient()
    
    try:
        payload = {
            "templateName": template_name,
            "subject": subject,
            "sender": {
                "name": sender_name,
                "email": final_sender_email
            },
            "htmlContent": html_content,
            "isActive": True
        }

        # Make the API request
        response = await client.request("/smtp/templates", method="POST", data=payload)
        
        # Response for create template is typically: {'id': 123}
        if isinstance(response, dict) and "id" in response:
             return json.dumps(response, indent=2)
        elif isinstance(response, dict):
             # Just return whatever valid JSON we got
             return json.dumps(response, indent=2)
        else:
             # Handle unexpected non-dict response
             return json.dumps({"status": "success", "response": str(response)})

    except BrevoApiError as e:
        # Re-raise Brevo API errors with full context or return structured error
        return json.dumps({
            "error": f"Brevo API Error: {e.message}",
            "details": e.details
        })
    except Exception as e:
        return json.dumps({
            "error": f"Exception calling Brevo: {str(e)}", 
            "traceback": traceback.format_exc()
        })
    finally:
        await client.close()
