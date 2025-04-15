import json
import os
import asyncio
from http import HTTPStatus

import httpx
from bs4 import BeautifulSoup
from supabase import create_client, Client

# --- Helper functions ---

async def fetch_kitespot_data(url: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text

async def parse_kitespot_data(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    # Example extraction: assume the kitespot name is in an <h1> tag and description in a <p> tag.
    name_elem = soup.find("h1")
    desc_elem = soup.find("p")
    return {
        "name": name_elem.text.strip() if name_elem else "Unknown",
        "description": desc_elem.text.strip() if desc_elem else "No description available"
    }

# --- Core crawler logic ---

async def crawl_kitespots() -> dict:
    # Example URL: Replace with your actual kitespot source URL(s)
    example_url = "https://www.example-kitespot.com"
    html = await fetch_kitespot_data(example_url)
    data = await parse_kitespot_data(html)
    data["source_url"] = example_url

    # --- Supabase Integration ---
    # Retrieve Supabase credentials from environment variables.
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_API_KEY = os.environ.get("SUPABASE_API_KEY")
    if not SUPABASE_URL or not SUPABASE_API_KEY:
        raise Exception("Supabase environment variables (SUPABASE_URL and SUPABASE_API_KEY) are not set")
    
    # Create the Supabase client.
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)
    
    # Insert the kitespot data into your "kitespots" table.
    response = supabase.table("kitespots").insert(data).execute()
    
    return {"data": data, "supabase_response": response.data}

# --- Vercel serverless function entry point ---

def handler(request):
    """
    Vercel serverless functions expect a `handler` function that
    receives a request object and returns a dict with HTTP status, headers, and body.
    """
    try:
        # Run the async crawler code synchronously using asyncio.run
        result = asyncio.run(crawl_kitespots())
        return {
            "statusCode": HTTPStatus.OK,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result)
        }
    except Exception as e:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }
