#!/usr/bin/env python3
"""
Improved Kitespot Discovery Agent

This module provides an asynchronous crawler to discover kitesurfing spots from various
web sources, enrich their data (such as geocoding locations), and insert new spots into a
Supabase database.

Requirements:
  - aiohttp
  - beautifulsoup4
  - fake_useragent
  - geopy
  - supabase
  - python-dotenv

Ensure that environment variables (e.g., SUPABASE_URL, SUPABASE_KEY) are set in a .env file.
"""

import asyncio
import aiohttp
import logging
import json
import re
import time
from typing import Dict, List, Optional, Any, Set
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from supabase import create_client, Client
from geopy.geocoders import Nominatim
import os
from dotenv import load_dotenv
from dataclasses import dataclass

# Configure logging: both to file and stream.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kitespot_discovery.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("kitespot-discovery")

# Load environment variables from .env file
load_dotenv()


@dataclass
class KitespotInfo:
    """Data class to store kitespot information."""
    name: str
    country: str
    latitude: Optional[float]
    longitude: Optional[float]
    description: Optional[str] = None
    difficulty: Optional[str] = None
    best_months: Optional[List[str]] = None
    source: Optional[str] = None
    source_url: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class KitespotDiscoveryCrawler:
    """Crawler to discover new kitespot records from various web sources and insert them into Supabase."""

    def __init__(self):
        """
        Initialize the crawler.
          - Create Supabase client.
          - Initialize geolocator.
          - Initialize user-agent rotator.
          - Define discovery sources and parsers.
          - Initialize caching for duplicate detection and geocoding.
        """
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        if not (self.supabase_url and self.supabase_key):
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in your environment.")
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        except TypeError:
            from supabase import Client, create_client as old_create_client
            self.supabase = old_create_client(self.supabase_url, self.supabase_key)

        self.geolocator = Nominatim(user_agent="kitespot_discovery_v1")
        self.user_agent = UserAgent()
        # Define discovery sources with dedicated parser functions.
        self.discovery_sources: Dict[str, Dict[str, Any]] = {
            "iksurf": {
                "url": "https://www.iksurf.com/kitesurf-spots/",
                "parser": self._parse_iksurf,
            },
            "kitesurfing_handbook": {
                "url": "https://kitesurfinghandbook.com/best-kitesurfing-spots/",
                "parser": self._parse_kitesurfing_handbook,
            },
            "inertia": {
                "url": "https://www.theinertia.com/travel/the-14-best-kiteboarding-spots-in-the-world/",
                "parser": self._parse_inertia,
            },
            "kiteboarder": {
                "url": "https://www.thekiteboarder.com/kiteboarding-travel-destinations/",
                "parser": self._parse_kiteboarder,
            },
            "mackiteboarding": {
                "url": "https://www.mackiteboarding.com/kiteboarding-spots/",
                "parser": self._parse_mackiteboarding,
            },
        }
        self.request_delay = 3.0  # Minimum seconds between requests.
        self.last_request_time = 0.0
        self.existing_spots_cache: Set[str] = set()  # To avoid duplicates.
        self.geocode_cache: Dict[str, Dict[str, float]] = {}

    async def _load_existing_kitespots(self):
        """
        Load existing kitespots from the Supabase 'kitespots' table and update the cache.
        The cache is based on a normalized key from the spot's name and country.
        """
        try:
            response = self.supabase.table("kitespots").select("name, country").execute()
            spots = response.data
            self.existing_spots_cache = {
                self._normalize_spot_key(spot["name"], spot.get("country", ""))
                for spot in spots
            }
            logger.info(f"Loaded {len(self.existing_spots_cache)} existing kitespots.")
        except Exception as e:
            logger.error(f"Error loading existing kitespots: {e}")
            self.existing_spots_cache = set()

    def _normalize_spot_key(self, name: str, country: str) -> str:
        """
        Create a normalized key for a kitespot by removing non-alphanumeric characters.
        """
        normalized_name = re.sub(r'[^a-z0-9]', '', name.lower())
        normalized_country = re.sub(r'[^a-z0-9]', '', country.lower()) if country else ""
        return f"{normalized_name}_{normalized_country}"

    def _spot_exists(self, name: str, country: str) -> bool:
        """
        Check if a kitespot already exists in the cache.
        """
        return self._normalize_spot_key(name, country) in self.existing_spots_cache

    async def _make_request(self, url: str) -> Optional[str]:
        """
        Asynchronously make an HTTP GET request with rate limiting and rotating user agents.
        Returns the HTML content as a string if successful.
        """
        # Enforce rate limiting.
        current_time = time.time()
        delay = self.request_delay - (current_time - self.last_request_time)
        if delay > 0:
            await asyncio.sleep(delay)

        headers = {
            "User-Agent": self.user_agent.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=30) as response:
                    self.last_request_time = time.time()
                    if response.status == 200:
                        logger.info(f"Fetched {url} successfully.")
                        return await response.text()
                    else:
                        logger.warning(f"Request to {url} returned status {response.status}.")
                        return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    async def _geocode_location(self, location_name: str) -> Optional[Dict[str, float]]:
        """
        Geocode a location name to obtain latitude and longitude.
        Uses caching to avoid duplicate geocoding requests.
        """
        if not location_name:
            return None

        if location_name in self.geocode_cache:
            return self.geocode_cache[location_name]

        # Basic rate limiting
        current_time = time.time()
        delay = self.request_delay - (current_time - self.last_request_time)
        if delay > 0:
            await asyncio.sleep(delay)

        try:
            location = self.geolocator.geocode(location_name, timeout=10)
            self.last_request_time = time.time()
            if location:
                coords = {"latitude": location.latitude, "longitude": location.longitude}
                self.geocode_cache[location_name] = coords
                logger.info(f"Geocoded location '{location_name}' successfully.")
                return coords
            logger.warning(f"No geocoding result for '{location_name}'.")
            return None
        except Exception as e:
            logger.error(f"Error geocoding '{location_name}': {e}")
            return None

    # -------------------- Parser Functions --------------------
    def _parse_iksurf(self, html: str, source_url: str) -> List[KitespotInfo]:
        """
        Parse kitespots from the IKSurf website.
        Returns a list of KitespotInfo objects.
        """
        spots = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            spot_elements = soup.select('.spot-listing')
            for element in spot_elements:
                try:
                    name_el = element.select_one('.spot-name')
                    loc_el = element.select_one('.spot-location')
                    if not (name_el and loc_el):
                        continue
                    name = name_el.get_text(strip=True)
                    location_text = loc_el.get_text(strip=True)
                    # Extract country using a simple comma-separated pattern.
                    country_match = re.search(r',\s*([^,]+)$', location_text)
                    country = country_match.group(1).strip() if country_match else ""
                    # Coordinates might be available as data attributes.
                    lat = float(element.get("data-lat")) if element.get("data-lat") else None
                    lng = float(element.get("data-lng")) if element.get("data-lng") else None
                    description_el = element.select_one('.spot-description')
                    description = description_el.get_text(strip=True) if description_el else None
                    difficulty_el = element.select_one('.spot-difficulty')
                    difficulty = difficulty_el.get_text(strip=True) if difficulty_el else None
                    months_el = element.select_one('.spot-months')
                    best_months = [m.strip() for m in months_el.get_text().split(',')] if months_el else None

                    spot = KitespotInfo(
                        name=name,
                        country=country,
                        latitude=lat,
                        longitude=lng,
                        description=description,
                        difficulty=difficulty,
                        best_months=best_months,
                        source="iksurf",
                        source_url=source_url,
                        metadata={}
                    )
                    spots.append(spot)
                except Exception as inner_e:
                    logger.error(f"Error parsing an element from IKSurf: {inner_e}")
                    continue
            logger.info(f"Extracted {len(spots)} spots from IKSurf.")
        except Exception as e:
            logger.error(f"Error parsing IKSurf HTML: {e}")
        return spots

    def _parse_kitesurfing_handbook(self, html: str, source_url: str) -> List[KitespotInfo]:
        """
        Parse kitesurf spots from Kitesurfing Handbook.
        Tries to extract spot names, countries, and descriptions.
        """
        spots = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Assume spot headings are in h2/h3; adjust selectors as needed.
            headings = soup.select("h2, h3")
            for heading in headings:
                try:
                    text = heading.get_text(strip=True)
                    # Skip non-spot headings (adjust patterns as necessary)
                    if not text or any(keyword in text.lower() for keyword in ['why', 'how', 'what', 'when', 'conclusion']):
                        continue

                    # Attempt to split name and country.
                    match = re.match(r'(.+?)(?:\s*[-–]\s*|\s+in\s+)(.+)', text)
                    if match:
                        name, country = match.groups()
                    else:
                        name = text
                        country = ""
                    # Optionally gather description from following paragraphs.
                    desc_parts = []
                    sibling = heading.find_next_sibling()
                    while sibling and sibling.name == "p":
                        desc_parts.append(sibling.get_text(strip=True))
                        sibling = sibling.find_next_sibling()
                    description = " ".join(desc_parts[:3]) if desc_parts else None

                    spot = KitespotInfo(
                        name=name,
                        country=country,
                        latitude=None,
                        longitude=None,
                        description=description,
                        source="kitesurfing_handbook",
                        source_url=source_url,
                        metadata={}
                    )
                    spots.append(spot)
                except Exception as inner_e:
                    logger.error(f"Error parsing a heading from Kitesurfing Handbook: {inner_e}")
                    continue
            logger.info(f"Extracted {len(spots)} spots from Kitesurfing Handbook.")
        except Exception as e:
            logger.error(f"Error parsing Kitesurfing Handbook HTML: {e}")
        return spots

    def _parse_inertia(self, html: str, source_url: str) -> List[KitespotInfo]:
        """
        Parse kitesurf spots from The Inertia website.
        """
        spots = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            headings = soup.select("h2, h3")
            for heading in headings:
                try:
                    text = heading.get_text(strip=True)
                    if not text or text.lower().startswith(('why', 'how', 'what', 'when')):
                        continue
                    match = re.match(r'(.+?)(?:,\s*|\s+in\s+)(.+)', text)
                    if match:
                        name, country = match.groups()
                    else:
                        name = text
                        country = ""
                    description = heading.find_next("p").get_text(strip=True) if heading.find_next("p") else None
                    spot = KitespotInfo(
                        name=name,
                        country=country,
                        latitude=None,
                        longitude=None,
                        description=description,
                        source="inertia",
                        source_url=source_url,
                        metadata={}
                    )
                    spots.append(spot)
                except Exception as inner_e:
                    logger.error(f"Error parsing a heading from The Inertia: {inner_e}")
                    continue
            logger.info(f"Extracted {len(spots)} spots from The Inertia.")
        except Exception as e:
            logger.error(f"Error parsing The Inertia HTML: {e}")
        return spots

    def _parse_kiteboarder(self, html: str, source_url: str) -> List[KitespotInfo]:
        """
        Parse kitesurf spots from The Kiteboarder website.
        """
        spots = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            articles = soup.select("article")
            for article in articles:
                try:
                    title_el = article.select_one("h2, h3")
                    if not title_el:
                        continue
                    title_text = title_el.get_text(strip=True)
                    match = re.match(r'(.+?)(?:,\s*|\s+in\s+)(.+)', title_text)
                    if match:
                        name, country = match.groups()
                    else:
                        name = title_text
                        country = ""
                    excerpt_el = article.select_one(".entry-summary, .excerpt")
                    description = excerpt_el.get_text(strip=True) if excerpt_el else None
                    spot = KitespotInfo(
                        name=name,
                        country=country,
                        latitude=None,
                        longitude=None,
                        description=description,
                        source="kiteboarder",
                        source_url=source_url,
                        metadata={}
                    )
                    spots.append(spot)
                except Exception as inner_e:
                    logger.error(f"Error parsing an article from The Kiteboarder: {inner_e}")
                    continue
            logger.info(f"Extracted {len(spots)} spots from The Kiteboarder.")
        except Exception as e:
            logger.error(f"Error parsing The Kiteboarder HTML: {e}")
        return spots

    def _parse_mackiteboarding(self, html: str, source_url: str) -> List[KitespotInfo]:
        """
        Parse kitesurf spots from Mackiteboarding website.
        """
        spots = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Try different selectors to cover layout variations.
            sections = soup.select(".spot-section, .location-section")
            if not sections:
                sections = soup.select("h2, h3")
            for section in sections:
                try:
                    text = section.get_text(strip=True)
                    if not text or text.lower().startswith(('why', 'how', 'what', 'when')):
                        continue
                    match = re.match(r'(.+?)(?:,\s*|\s+in\s+)(.+)', text)
                    if match:
                        name, country = match.groups()
                    else:
                        name = text
                        country = ""
                    next_p = section.find_next("p")
                    description = next_p.get_text(strip=True) if next_p else None
                    spot = KitespotInfo(
                        name=name,
                        country=country,
                        latitude=None,
                        longitude=None,
                        description=description,
                        source="mackiteboarding",
                        source_url=source_url,
                        metadata={}
                    )
                    spots.append(spot)
                except Exception as inner_e:
                    logger.error(f"Error parsing a section from Mackiteboarding: {inner_e}")
                    continue
            logger.info(f"Extracted {len(spots)} spots from Mackiteboarding.")
        except Exception as e:
            logger.error(f"Error parsing Mackiteboarding HTML: {e}")
        return spots

    # -------------------- Geocode and Insert --------------------
    async def _geocode_and_insert(self, spot: KitespotInfo):
        """
        Geocode a single kitespot and insert it into Supabase if it doesn't already exist.
        """
        try:
            search_query = f"{spot.name}, {spot.country} kitesurfing spot"
            geocoded = await self._geocode_location(search_query)
            if geocoded:
                spot.latitude = geocoded.get("latitude")
                spot.longitude = geocoded.get("longitude")
            else:
                logger.error(f"Geocoding failed for spot: {spot.name}, {spot.country}")
                return

            # Check cache to avoid duplicate insertions.
            if self._spot_exists(spot.name, spot.country):
                logger.info(f"Skipping duplicate spot: {spot.name}, {spot.country}")
                return

            # Prepare the data for insertion.
            spot_data = {
                "name": spot.name,
                "country": spot.country,
                "latitude": spot.latitude,
                "longitude": spot.longitude,
                "description": spot.description,
                "difficulty": spot.difficulty,
                "best_months": json.dumps(spot.best_months) if spot.best_months else None,
                "source": spot.source,
                "source_url": spot.source_url,
                # Additional metadata can be added here.
            }

            response = self.supabase.table("kitespots").insert(spot_data).execute()
            if response.data:
                logger.info(f"Inserted new spot: {spot.name}, {spot.country}")
                self.existing_spots_cache.add(self._normalize_spot_key(spot.name, spot.country))
            else:
                logger.error(f"Insertion failed for {spot.name}, {spot.country}: {response.error}")
        except Exception as e:
            logger.error(f"Error in geocoding/inserting spot '{spot.name}': {e}")

    async def discover_and_insert_kitespots(self):
        """
        Main function to orchestrate the discovery and insertion of new kitespots.
          - Loads existing spots.
          - Loops through each discovery source.
          - Fetches and parses content.
          - Concurrently geocodes and inserts the discovered spots.
        """
        try:
            await self._load_existing_kitespots()
            tasks = []

            for source_name, source_info in self.discovery_sources.items():
                url = source_info["url"]
                parser = source_info["parser"]

                html = await self._make_request(url)
                if html:
                    spots = parser(html, url)
                    logger.info(f"Found {len(spots)} spots from {source_name}.")
                    for spot in spots:
                        tasks.append(self._geocode_and_insert(spot))
                else:
                    logger.error(f"Failed to retrieve data from {url}")

            if tasks:
                await asyncio.gather(*tasks)
            else:
                logger.warning("No new spots discovered from any source.")
        except Exception as e:
            logger.error(f"Error in discover_and_insert_kitespots: {e}")


async def main():
    """Main function to run the crawler."""
    crawler = KitespotDiscoveryCrawler()
    await crawler.discover_and_insert_kitespots()

if __name__ == "__main__":
    asyncio.run(main())