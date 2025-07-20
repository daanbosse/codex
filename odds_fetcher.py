import asyncio
import os
import logging
import time
import re
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from typing import List, Dict

logger = logging.getLogger(__name__)

# Environment variables
PROXY_POOL_URL = os.getenv('PROXY_POOL_URL')
PROXY_USER = os.getenv('PROXY_USER')
PROXY_PASS = os.getenv('PROXY_PASS')
URL_BET365 = os.getenv('URL_BET365')
URL_TOTO = os.getenv('URL_TOTO')

async def fetch_bet365() -> List[Dict]:
    """
    Fetches squash match odds from Bet365 site.

    Returns:
        list: A list of dicts containing match_id, player_A, player_B, odds_A, odds_B, and timestamp.
    """
    if not URL_BET365:
        logger.warning("URL_BET365 environment variable not set, returning empty list")
        return []

    return await fetch_odds(URL_BET365, "bet365")

async def fetch_toto() -> List[Dict]:
    """
    Fetches squash match odds from Toto site.

    Returns:
        list: A list of dicts containing match_id, player_A, player_B, odds_A, odds_B, and timestamp.
    """
    if not URL_TOTO:
        logger.warning("URL_TOTO environment variable not set, returning empty list")
        return []

    return await fetch_odds(URL_TOTO, "toto")

async def fetch_odds(url: str, site: str) -> List[Dict]:
    """
    Generic function to fetch squash odds from a given URL.

    Args:
        url (str): The URL to navigate to.
        site (str): The name of the betting site.

    Returns:
        list: A list of dicts containing match_id, player_A, player_B, odds_A, odds_B, and timestamp.
    """
    if not url:
        logger.error(f"No URL provided for {site}")
        return []

    logger.info(f"Fetching odds from {site}: {url}")

    try:
        async with async_playwright() as p:
            # Configure proxy if available
            proxy_config = None
            if PROXY_POOL_URL and PROXY_USER and PROXY_PASS:
                proxy_config = {
                    'server': PROXY_POOL_URL,
                    'username': PROXY_USER,
                    'password': PROXY_PASS
                }
                logger.info(f"Using proxy for {site}")

            browser = await p.chromium.launch(
                proxy=proxy_config,
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()

            try:
                # Navigate to the URL with timeout
                await page.goto(url, timeout=30000)
                logger.info(f"Successfully navigated to {site}")

                # Try to wait for page content, but fallback if it takes too long
                try:
                    await page.wait_for_load_state('domcontentloaded', timeout=10000)
                    logger.info(f"Page content loaded for {site}")
                except PlaywrightTimeoutError:
                    logger.warning(f"Page load timeout for {site}, proceeding anyway")

                # Parse the HTML content based on the site
                if site == "bet365":
                    return await parse_bet365_html(page)
                elif site == "toto":
                    return await parse_toto_html(page)
                else:
                    logger.warning(f"No parser implemented for {site}")
                    return []

            except PlaywrightTimeoutError:
                logger.warning(f"Timeout while fetching data from {site} ({url}), returning empty list")
                return []
            except Exception as e:
                logger.error(f"Error fetching data from {site}: {e}")
                return []
            finally:
                await browser.close()

    except Exception as e:
        logger.error(f"Failed to launch browser for {site}: {e}")
        return []

async def parse_bet365_html(page) -> List[Dict]:
    """Parse bet365 HTML to extract squash odds."""
    try:
        # Wait for page to load completely
        await page.wait_for_load_state('networkidle', timeout=15000)
        
        # Log page title and URL for debugging
        title = await page.title()
        url = page.url
        logger.info(f"Bet365 page loaded: {title} at {url}")
        
        # Get page content for debugging
        content = await page.content()
        logger.info(f"Page content length: {len(content)} characters")
        
        # Try to find common betting site elements
        possible_selectors = [
            '.gl-MarketGroup',
            '.event-holder',
            '.coupon-content',
            '.market-group',
            '[class*="market"]',
            '[class*="event"]',
            '[class*="match"]',
            '[class*="fixture"]'
        ]
        
        matches = []
        for selector in possible_selectors:
            try:
                elements = await page.query_selector_all(selector)
                if elements:
                    logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    matches = elements
                    break
            except Exception as e:
                logger.debug(f"Selector {selector} failed: {e}")
        
        if not matches:
            logger.warning("No match elements found on bet365, logging page structure")
            # Log first 1000 characters of body content for debugging
            body_content = await page.evaluate('() => document.body.innerText.substring(0, 1000)')
            logger.info(f"Page body content preview: {body_content}")
            return []
        
        odds_data: List[Dict] = []
        for idx, match in enumerate(matches):
            try:
                text = (await match.inner_text()).strip()
                if not text:
                    continue

                # Attempt to extract players in "A v B" or "A vs B" format
                player_match = re.search(r"([A-Za-z .'-]+)\s+v(?:s)?\.?\s+([A-Za-z .'-]+)", text, re.IGNORECASE)
                if not player_match:
                    continue
                player_A = player_match.group(1).strip()
                player_B = player_match.group(2).strip()

                # Extract numeric odds from the text
                odds = re.findall(r"\d+(?:\.\d+)?", text)
                if len(odds) < 2:
                    continue
                odds_A = float(odds[0])
                odds_B = float(odds[1])

                match_id = await match.get_attribute('data-fixtureid')
                if not match_id:
                    match_id = await match.get_attribute('id') or f"bet365_{idx}"

                odds_data.append({
                    'match_id': match_id,
                    'player_A': player_A,
                    'player_B': player_B,
                    'odds_A': odds_A,
                    'odds_B': odds_B,
                    'timestamp': str(int(time.time())),
                    'site': 'bet365'
                })
            except Exception as e:
                logger.debug(f"Failed to parse a bet365 match element: {e}")
                continue

        logger.info(f"Parsed {len(odds_data)} bet365 matches")
        return odds_data

    except Exception as e:
        logger.error(f"Error parsing bet365 HTML: {e}")
        return []

async def parse_toto_html(page) -> List[Dict]:
    """Parse toto HTML to extract squash odds."""
    try:
        # Wait for page to load completely
        await page.wait_for_load_state('networkidle', timeout=15000)
        
        # Log page title and URL for debugging
        title = await page.title()
        url = page.url
        logger.info(f"Toto page loaded: {title} at {url}")
        
        # Get page content for debugging
        content = await page.content()
        logger.info(f"Page content length: {len(content)} characters")
        
        # Try to find common betting site elements
        possible_selectors = [
            '.event-row',
            '.match-row',
            '.fixture',
            '.game-row',
            '[class*="event"]',
            '[class*="match"]',
            '[class*="game"]',
            '[class*="fixture"]',
            '.odds-row'
        ]
        
        matches = []
        for selector in possible_selectors:
            try:
                elements = await page.query_selector_all(selector)
                if elements:
                    logger.info(f"Found {len(elements)} elements with selector: {selector}")
                    matches = elements
                    break
            except Exception as e:
                logger.debug(f"Selector {selector} failed: {e}")
        
        if not matches:
            logger.warning("No match elements found on toto, logging page structure")
            # Log first 1000 characters of body content for debugging
            body_content = await page.evaluate('() => document.body.innerText.substring(0, 1000)')
            logger.info(f"Page body content preview: {body_content}")
            return []
        
        odds_data: List[Dict] = []
        for idx, match in enumerate(matches):
            try:
                text = (await match.inner_text()).strip()
                if not text:
                    continue

                player_match = re.search(r"([A-Za-z .'-]+)\s+v(?:s)?\.?\s+([A-Za-z .'-]+)", text, re.IGNORECASE)
                if not player_match:
                    continue
                player_A = player_match.group(1).strip()
                player_B = player_match.group(2).strip()

                odds = re.findall(r"\d+(?:\.\d+)?", text)
                if len(odds) < 2:
                    continue
                odds_A = float(odds[0])
                odds_B = float(odds[1])

                match_id = await match.get_attribute('data-event-id')
                if not match_id:
                    match_id = await match.get_attribute('id') or f"toto_{idx}"

                odds_data.append({
                    'match_id': match_id,
                    'player_A': player_A,
                    'player_B': player_B,
                    'odds_A': odds_A,
                    'odds_B': odds_B,
                    'timestamp': str(int(time.time())),
                    'site': 'toto'
                })
            except Exception as e:
                logger.debug(f"Failed to parse a toto match element: {e}")
                continue

        logger.info(f"Parsed {len(odds_data)} toto matches")
        return odds_data

    except Exception as e:
        logger.error(f"Error parsing toto HTML: {e}")
        return []

async def parse_squash_odds(raw_data: Dict) -> List[Dict]:
    """
    Parses the raw JSON data from squash sites.

    Args:
        raw_data (dict): The raw JSON data.

    Returns:
        list: A list of dicts containing match_id, player_A, player_B, odds_A, odds_B, and timestamp.
    """
    if not raw_data or not isinstance(raw_data, dict):
        logger.warning("Invalid or empty raw data provided")
        return []

    parsed_data = []

    try:
        for match in raw_data.get('matches', []):
            try:
                match_details = {
                    'match_id': match['id'],
                    'player_A': match['playerA']['name'],
                    'player_B': match['playerB']['name'],
                    'odds_A': float(match['odds']['playerA']),
                    'odds_B': float(match['odds']['playerB']),
                    'timestamp': match.get('timestamp', ''),
                    'site': raw_data.get('site', 'unknown')
                }
                parsed_data.append(match_details)
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"Error parsing individual match data: {e}")
                continue

    except Exception as e:
        logger.error(f"Error parsing odds data: {e}")

    logger.info(f"Successfully parsed {len(parsed_data)} matches")
    return parsed_data
