# Odds Fetcher

This repository contains asynchronous helpers for scraping squash betting odds using [Playwright](https://playwright.dev/). The main functions are `fetch_bet365` and `fetch_toto` defined in `odds_fetcher.py`.

## Environment Variables

Set the following variables before running the scripts:

- `URL_BET365` – URL for the Bet365 page to scrape.
- `URL_TOTO` – URL for the Toto page to scrape.
- `PROXY_POOL_URL` – proxy server address. Optional but recommended when scraping.
- `PROXY_USER` – username for the proxy.
- `PROXY_PASS` – password for the proxy.

### Example using a Decodo proxy
```bash
export PROXY_POOL_URL="http://gate.dc.smartproxy.com:10001"
export PROXY_USER="user123"
export PROXY_PASS="pass123"
```
Replace the values with your Decodo credentials.

## Installation

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Install Playwright browsers:
   ```bash
   playwright install
   ```

## Usage

Create a small runner script or use the Python REPL:

```python
import asyncio
from odds_fetcher import fetch_bet365, fetch_toto

async def main():
    bet365_data = await fetch_bet365()
    toto_data = await fetch_toto()
    print("Bet365", bet365_data)
    print("Toto", toto_data)

asyncio.run(main())
```

Ensure the environment variables are set beforehand. The functions return lists of dictionaries with the odds information.
