from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse, parse_qs
from Client.Linkly_client import LinklyApiClient
from Error.linkly_error import LinklyApiError
import asyncio
import logging

async def track_link_clicks(
    campaign_id: Optional[str] = None,
    link_ids: Optional[List[int]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    country: Optional[str] = None,
    exclude_bots: bool = True,
    unique_only: bool = True,
    frequency: str = "day",
    debug: bool = False
) -> dict:
    """
    Track link clicks and campaign engagement using the Linkly API.
    
    PARALLEL APPROACH: Makes all API calls in parallel using asyncio.gather
    to minimize total execution time and stay within rate limits.
    """

    client = LinklyApiClient()
    
    try:
        # Default to last 30 days
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        debug_info = {
            "steps": [],
            "links_found": [],
            "campaign_search": campaign_id,
            "api_calls_made": 0
        }
        
        # Step 1: Get link IDs if campaign_id provided but no link_ids
        if campaign_id and not link_ids:
            try:
                links_endpoint = f"/api/v1/workspace/{client.workspace_id}/links"
                debug_info["steps"].append(f"Fetching links from: {links_endpoint}")
                debug_info["api_calls_made"] += 1
                
                links_response = await client.request(links_endpoint, method="GET")
                
                link_ids = []
                links_data = []
                
                # Handle different response formats
                if isinstance(links_response, list):
                    links_data = links_response
                elif isinstance(links_response, dict):
                    links_data = (links_response.get("links") or 
                                 links_response.get("data") or 
                                 links_response.get("results") or [])
                
                debug_info["steps"].append(f"Got {len(links_data)} total links")
                
                # Find links matching the campaign
                for link in links_data:
                    link_destination = link.get("destination", "")
                    link_formatted = link.get("formatted_url", "")
                    link_url = link.get("url", "")
                    link_id = link.get("id") or link.get("link_id")
                    
                    all_urls = f"{link_destination} {link_formatted} {link_url}"
                    
                    patterns = [
                        f"campaign={campaign_id}",
                        f"campaign_id={campaign_id}",
                        f"campaignId={campaign_id}",
                        campaign_id
                    ]
                    
                    if any(pattern in all_urls for pattern in patterns):
                        if link_id:
                            link_ids.append(str(link_id))
                            if debug:
                                debug_info["steps"].append(f"✓ Matched link {link_id}")
                
                debug_info["total_links_checked"] = len(links_data)
                debug_info["matching_links_found"] = len(link_ids)
                
                if not link_ids:
                    return {
                        "status": "no_links_found",
                        "message": f"No links found for campaign '{campaign_id}'",
                        "campaign_id": campaign_id,
                        "debug_info": debug_info if debug else None
                    }
                    
            except Exception as e:
                import traceback
                return {
                    "status": "error_fetching_links",
                    "error": str(e),
                    "traceback": traceback.format_exc() if debug else None,
                    "debug_info": debug_info if debug else None
                }
        
        if not link_ids:
            return {
                "status": "missing_parameters",
                "message": "Please provide either campaign_id or link_ids"
            }
        
        debug_info["link_ids_to_check"] = link_ids
        
        # ============================================================
        # PARALLEL API CALLS: Fetch all links simultaneously
        # ============================================================
        
        if debug:
            debug_info["steps"].append(f"\n⚡ Fetching clicks for {len(link_ids)} links in PARALLEL...")
        
        # Build base params (correct parameter names per API docs)
        base_params = {
            "unique": "true" if unique_only else "false",
            "format": "json",
            "frequency": frequency,
            "start": start_date,  # API uses 'start', not 'from'
            "end": end_date,      # API uses 'end', not 'to'
            "bots": "false" if exclude_bots else "true"  # API uses 'bots' parameter
        }
        
        if country:
            base_params["country"] = country.upper()
        
        # Helper function to fetch clicks for a single link
        async def fetch_link_clicks(link_id: str):
            """Fetch clicks for a single link"""
            params = base_params.copy()
            params["link_id"] = str(link_id)  # Use singular 'link_id' for individual calls
            
            # Use params dict, let client handle encoding and api_key
            endpoint = f"/api/v1/workspace/{client.workspace_id}/clicks"
            
            try:
                # Log what we are about to query
                if debug:
                    logging.info(f"Fetching for Link ID: {link_id}")
                    
                result = await client.request(endpoint, method="GET", params=params)
                
                click_count = 0
                link_clicks = []
                
                if isinstance(result, dict):
                    # Traffic data (aggregated format)
                    if "traffic" in result:
                        traffic_data = result.get("traffic", [])
                        click_count = sum(item.get("y", 0) for item in traffic_data if isinstance(item, dict))
                    
                    # Raw click data
                    elif "clicks" in result or "data" in result:
                        link_clicks = result.get("clicks", []) or result.get("data", [])
                        click_count = len(link_clicks)
                
                elif isinstance(result, list):
                    link_clicks = result
                    click_count = len(result)
                
                return {
                    "link_id": link_id,
                    "click_count": click_count,
                    "clicks": link_clicks,
                    "error": None
                }
                
            except LinklyApiError as e:
                return {
                    "link_id": link_id,
                    "click_count": 0,
                    "clicks": [],
                    "error": f"{e.status_code}: {e.message}"
                }
            except Exception as e:
                return {
                    "link_id": link_id,
                    "click_count": 0,
                    "clicks": [],
                    "error": str(e)
                }
        
        # Execute all requests in parallel
        results = await asyncio.gather(*[fetch_link_clicks(link_id) for link_id in link_ids])
        debug_info["api_calls_made"] += len(link_ids)
        
        # Process results
        clicks_per_link = {}
        all_clicks = []
        errors_per_link = {}
        
        for result in results:
            link_id = result["link_id"]
            click_count = result["click_count"]
            clicks = result["clicks"]
            error = result["error"]
            
            clicks_per_link[link_id] = click_count
            
            if clicks:
                all_clicks.extend(clicks)
            
            if error:
                errors_per_link[link_id] = error
                if debug:
                    debug_info["steps"].append(f"  ✗ Link {link_id}: {error}")
            elif debug and click_count > 0:
                debug_info["steps"].append(f"  ✓ Link {link_id}: {click_count} clicks")
        
        # Calculate total
        total_clicks = sum(clicks_per_link.values())
        
        if debug:
            debug_info["steps"].append(f"\n📊 Total clicks: {total_clicks}")
            debug_info["steps"].append(f"Total API calls: {debug_info['api_calls_made']}")
            debug_info["execution_mode"] = "parallel"
            debug_info["clicks_per_link"] = clicks_per_link
            if errors_per_link:
                debug_info["errors_per_link"] = errors_per_link
        
        # Check for complete failure
        if errors_per_link and total_clicks == 0 and len(errors_per_link) == len(link_ids):
            return {
                "status": "error",
                "message": "Failed to fetch clicks for all links",
                "errors": errors_per_link,
                "debug_info": debug_info if debug else None
            }
        
        # No clicks found
        if total_clicks == 0:
            return {
                "status": "no_clicks",
                "message": "No clicks recorded yet for these links",
                "campaign_id": campaign_id,
                "date_range": f"{start_date} to {end_date}",
                "link_ids_checked": link_ids,
                "total_links": len(link_ids),
                "total_clicks": 0,
                "clicks_per_link": clicks_per_link,
                "filters_applied": {
                    "country": country,
                    "unique_only": unique_only,
                    "exclude_bots": exclude_bots
                },
                "debug_info": debug_info if debug else None,
                "suggestion": "No clicks found with current filters. Try removing filters to see all clicks."
            }
        
        # SUCCESS - Analyze clicks
        clicks_by_contact = {}
        clicks_by_date = {}
        clicks_by_country = {}
        clicks_by_device = {}
        clicks_by_browser = {}
        unique_ips = set()
        
        for click in all_clicks:
            if not isinstance(click, dict):
                continue
            
            # Extract email from destination URL
            dest_url = click.get("destination", "") or click.get("url", "")
            email = None
            
            if "email=" in dest_url:
                try:
                    parsed = urlparse(dest_url)
                    params_parsed = parse_qs(parsed.query)
                    email = params_parsed.get("email", [None])[0]
                    if email:
                        email = email.strip()
                except:
                    pass
            
            # Track by contact email
            if email:
                if email not in clicks_by_contact:
                    clicks_by_contact[email] = {
                        "total_clicks": 0,
                        "first_clicked_at": None,
                        "last_clicked_at": None,
                        "country": click.get("country", "Unknown"),
                        "device": click.get("device", "Unknown"),
                        "browser": click.get("browser", "Unknown"),
                        "city": click.get("city", "Unknown")
                    }
                
                clicks_by_contact[email]["total_clicks"] += 1
                
                timestamp = click.get("timestamp") or click.get("created_at") or click.get("clickedAt")
                if timestamp:
                    if not clicks_by_contact[email]["first_clicked_at"]:
                        clicks_by_contact[email]["first_clicked_at"] = timestamp
                    clicks_by_contact[email]["last_clicked_at"] = timestamp
            
            # Track by date
            timestamp = click.get("timestamp") or click.get("created_at") or ""
            if timestamp:
                date = str(timestamp).split("T")[0]
                clicks_by_date[date] = clicks_by_date.get(date, 0) + 1
            
            # Track by country
            country_code = click.get("country", "Unknown")
            clicks_by_country[country_code] = clicks_by_country.get(country_code, 0) + 1
            
            # Track by device
            device = click.get("device", "Unknown")
            clicks_by_device[device] = clicks_by_device.get(device, 0) + 1
            
            # Track by browser
            browser = click.get("browser", "Unknown")
            clicks_by_browser[browser] = clicks_by_browser.get(browser, 0) + 1
            
            # Track unique IPs
            ip = click.get("ip") or click.get("ipAddress")
            if ip:
                unique_ips.add(ip)
        
        # Calculate engagement rate
        engagement_rate = (len(clicks_by_contact) / len(link_ids) * 100) if link_ids else 0
        
        # Build response
        result = {
            "status": "success",
            "campaign_id": campaign_id,
            "date_range": {
                "start": start_date,
                "end": end_date
            },
            "summary": {
                "total_clicks": total_clicks,
                "unique_visitors": len(unique_ips) if unique_ips else total_clicks,
                "contacts_who_clicked": len(clicks_by_contact),
                "links_tracked": len(link_ids),
                "engagement_rate": f"{engagement_rate:.1f}%"
            },
            "clicks_per_link": clicks_per_link,
            "analytics": {
                "by_contact": clicks_by_contact,
                "by_date": dict(sorted(clicks_by_date.items())) if clicks_by_date else {},
                "by_country": clicks_by_country,
                "by_device": clicks_by_device,
                "by_browser": clicks_by_browser
            },
            "debug_info": debug_info if debug else None
        }
        
        # Add top performers
        if clicks_by_contact:
            result["top_performers"] = {
                "most_engaged": max(clicks_by_contact.items(), key=lambda x: x[1]["total_clicks"])[0],
                "most_engaged_clicks": max(c["total_clicks"] for c in clicks_by_contact.values())
            }
        
        if clicks_by_date:
            result["top_performers"] = result.get("top_performers", {})
            result["top_performers"]["most_active_day"] = max(clicks_by_date.items(), key=lambda x: x[1])[0]
        
        return result
        
    except LinklyApiError as e:
        return {
            "status": "error",
            "error_type": "LinklyApiError",
            "message": e.message,
            "status_code": e.status_code
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc()
        }
    finally:
        await client.close()