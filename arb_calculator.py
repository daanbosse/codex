
import asyncio
import os
import redis.asyncio as redis
import asyncpg
import aiohttp
import logging
from typing import List, Dict, Optional
from odds_fetcher import fetch_bet365, fetch_toto

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
REDIS_URL = os.getenv('REDIS_URL')
DATABASE_URL = os.getenv('DATABASE_URL')
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK')

def validate_configuration():
    """Validate that all required environment variables are set."""
    missing_vars = []
    
    if not REDIS_URL:
        missing_vars.append('REDIS_URL')
    if not DATABASE_URL:
        missing_vars.append('DATABASE_URL')
    if not DISCORD_WEBHOOK:
        missing_vars.append('DISCORD_WEBHOOK')
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("All required environment variables are configured")
    return True

async def compute_arbitrage(odds_list1: List[Dict], odds_list2: List[Dict]) -> List[Dict]:
    """
    Calculate arbitrage opportunities between two lists of odds.
    Returns a list of dictionaries with match_id and arbitrage percentage if found.
    """
    if not odds_list1 or not odds_list2:
        logger.warning("One or both odds lists are empty")
        return []
    
    results = []
    match_ids = {odds1['match_id'] for odds1 in odds_list1} & {odds2['match_id'] for odds2 in odds_list2}
    
    logger.info(f"Found {len(match_ids)} common matches between betting sites")
    
    for match_id in match_ids:
        try:
            odds1 = next(odds for odds in odds_list1 if odds['match_id'] == match_id)
            odds2 = next(odds for odds in odds_list2 if odds['match_id'] == match_id)
            
            # Calculate implied probabilities and check for arbitrage
            implied_sum1 = 1 / odds1['odds_A'] + 1 / odds2['odds_B']
            implied_sum2 = 1 / odds1['odds_B'] + 1 / odds2['odds_A']
            
            if implied_sum1 < 1:
                arb_percent = (1 - implied_sum1) * 100
                results.append({
                    'match_id': match_id,
                    'arb_percent': round(arb_percent, 2),
                    'site1': odds1.get('site', 'Site1'),
                    'site2': odds2.get('site', 'Site2'),
                    'player_A': odds1.get('player_A', 'Player A'),
                    'player_B': odds1.get('player_B', 'Player B'),
                    'strategy': 'A1_B2',
                    'odds_details': {
                        'odds_A1': odds1['odds_A'],
                        'odds_B2': odds2['odds_B'],
                    }
                })
                
            if implied_sum2 < 1:
                arb_percent = (1 - implied_sum2) * 100
                results.append({
                    'match_id': match_id,
                    'arb_percent': round(arb_percent, 2),
                    'site1': odds1.get('site', 'Site1'),
                    'site2': odds2.get('site', 'Site2'),
                    'player_A': odds1.get('player_A', 'Player A'),
                    'player_B': odds1.get('player_B', 'Player B'),
                    'strategy': 'B1_A2',
                    'odds_details': {
                        'odds_B1': odds1['odds_B'],
                        'odds_A2': odds2['odds_A'],
                    }
                })
                
        except (KeyError, ZeroDivisionError, TypeError) as e:
            logger.error(f"Error calculating arbitrage for match {match_id}: {e}")
            continue
    
    logger.info(f"Found {len(results)} arbitrage opportunities")
    return results

async def connect_to_redis() -> Optional[redis.Redis]:
    """Connects to Redis and returns a Redis client."""
    try:
        client = redis.from_url(
            REDIS_URL,
            decode_responses=True
        )
        # Test the connection
        await client.ping()
        logger.info("Successfully connected to Redis")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None

async def store_arbitrage_alert(redis_client: redis.Redis, alert: Dict) -> bool:
    """Stores the arbitrage alert in Redis to prevent duplicates."""
    try:
        key = f"arb_alert:{alert['match_id']}:{alert['strategy']}"
        await redis_client.setex(key, 3600, "processed")  # Expire after 1 hour
        return True
    except Exception as e:
        logger.error(f"Failed to store alert in Redis: {e}")
        return False

async def connect_to_postgres() -> Optional[asyncpg.Pool]:
    """Connects to the Postgres database and returns the connection pool."""
    try:
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=30
        )
        logger.info("Successfully connected to PostgreSQL")
        return pool
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return None

async def send_discord_alert(alert: Dict) -> bool:
    """Sends an alert to a Discord webhook."""
    try:
        import datetime
        
        embed = {
            "embeds": [{
                "title": "🚨 Arbitrage Opportunity Found!",
                "color": 0x00ff00,
                "fields": [
                    {"name": "Match", "value": f"{alert['player_A']} vs {alert['player_B']}", "inline": False},
                    {"name": "Arbitrage %", "value": f"{alert['arb_percent']}%", "inline": True},
                    {"name": "Strategy", "value": alert['strategy'], "inline": True},
                    {"name": "Sites", "value": f"{alert['site1']} vs {alert['site2']}", "inline": True},
                    {"name": "Odds", "value": str(alert['odds_details']), "inline": False}
                ],
                "timestamp": datetime.datetime.utcnow().isoformat()
            }]
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK, json=embed) as response:
                if response.status == 204:
                    logger.info(f"Discord alert sent for match {alert['match_id']}")
                    return True
                else:
                    logger.error(f"Discord webhook failed with status {response.status}")
                    response_text = await response.text()
                    logger.error(f"Discord response: {response_text}")
                    return False
                    
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")
        return False

async def run_cycle():
    """Run a cycle of fetching, computing, and alerting for arbitrage opportunities."""
    logger.info("Starting arbitrage scan cycle")
    
    # Validate configuration
    if not validate_configuration():
        logger.error("Configuration validation failed. Exiting.")
        return
    
    try:
        # Fetch odds data
        logger.info("Fetching odds from betting sites...")
        bet365_odds, toto_odds = await asyncio.gather(
            fetch_bet365(),
            fetch_toto(),
            return_exceptions=True
        )
        
        # Handle fetch errors
        if isinstance(bet365_odds, Exception):
            logger.error(f"Failed to fetch Bet365 odds: {bet365_odds}")
            bet365_odds = []
        
        if isinstance(toto_odds, Exception):
            logger.error(f"Failed to fetch Toto odds: {toto_odds}")
            toto_odds = []
        
        if not bet365_odds and not toto_odds:
            logger.error("Failed to fetch odds from both sites")
            return
        
        logger.info(f"Fetched {len(bet365_odds)} Bet365 odds and {len(toto_odds)} Toto odds")
        
        # Compute arbitrage opportunities
        alerts = await compute_arbitrage(bet365_odds, toto_odds)
        
        if not alerts:
            logger.info("No arbitrage opportunities found")
            return
        
        # Connect to services
        redis_client = await connect_to_redis()
        pg_pool = await connect_to_postgres()
        
        if not redis_client:
            logger.warning("Redis connection failed, proceeding without duplicate checking")
        
        if not pg_pool:
            logger.warning("PostgreSQL connection failed, alerts will not be stored in database")
        
        # Process alerts
        alerts_sent = 0
        for alert in alerts:
            try:
                # Check for duplicates if Redis is available
                if redis_client:
                    key = f"arb_alert:{alert['match_id']}:{alert['strategy']}"
                    if await redis_client.exists(key):
                        logger.debug(f"Skipping duplicate alert for {alert['match_id']}")
                        continue
                
                # Send Discord alert
                if await send_discord_alert(alert):
                    alerts_sent += 1
                    
                    # Store in Redis to prevent duplicates
                    if redis_client:
                        await store_arbitrage_alert(redis_client, alert)
                        
                    # Store in database if available
                    if pg_pool:
                        async with pg_pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO arbitrage_alerts 
                                (match_id, arb_percent, site1, site2, strategy, odds_details, created_at)
                                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                                ON CONFLICT (match_id, strategy) DO NOTHING
                            """, alert['match_id'], alert['arb_percent'], alert['site1'], 
                            alert['site2'], alert['strategy'], str(alert['odds_details']))
                            
            except Exception as e:
                logger.error(f"Error processing alert for match {alert['match_id']}: {e}")
                continue
        
        logger.info(f"Arbitrage scan complete. Sent {alerts_sent} alerts out of {len(alerts)} opportunities")
        
    except Exception as e:
        logger.error(f"Unexpected error during cycle: {e}")
        raise
    
    finally:
        # Clean up connections
        if 'redis_client' in locals() and redis_client:
            await redis_client.close()
        if 'pg_pool' in locals() and pg_pool:
            await pg_pool.close() 
