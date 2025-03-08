import requests
import time
import json
import logging
import os
import queue
import aiohttp
import asyncio
import sys
import threading
from dotenv import load_dotenv
from hyperliquid.websocket_manager import WebsocketManager
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import scrolledtext
from datetime import datetime
from eth_account import Account as EthAccount
from eth_account.messages import encode_defunct
from starknet_py.common import int_from_bytes
from utils import build_auth_message, get_account
from shared.api_client import get_paradex_config
import hmac
import hashlib

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
eth_private_key = os.getenv("ETHEREUM_PRIVATE_KEY")
logger.debug(f"Loaded ETHEREUM_PRIVATE_KEY: {eth_private_key}")
paradex_starknet_address = os.getenv("PARADEX_STARKNET_ADDRESS")
paradex_starknet_private_key = os.getenv("PARADEX_STARKNET_PRIVATE_KEY")
hyperliquid_api_key = os.getenv("HYPERLIQUID_API_KEY")
hyperliquid_api_secret = os.getenv("HYPERLIQUID_API_SECRET")
hyperliquid_wallet_address = os.getenv("HYPERLIQUID_WALLET_ADDRESS", "0xmockedaddress")

paradex_http_url = "https://api.prod.paradex.trade/v1"

class ParadexClient:
    def __init__(self):
        if not paradex_starknet_address or not paradex_starknet_private_key:
            raise ValueError("PARADEX_STARKNET_ADDRESS and PARADEX_STARKNET_PRIVATE_KEY must be set in .env file")

        # Initialize with minimal setup to fetch config
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.paradex_config = loop.run_until_complete(get_paradex_config(paradex_http_url))
        loop.close()

        # Get JWT token for authentication
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.jwt = loop.run_until_complete(self._get_jwt_token(self.paradex_config, paradex_starknet_address, paradex_starknet_private_key))
        self.headers = {"Authorization": f"Bearer {self.jwt}", "Content-Type": "application/json"}
        loop.close()

        self.base_url = paradex_http_url
        self.balances = {"USDC": 0}
        self.available_collateral = 0
        self.markets = set()
        self.fees = {"maker": -0.00005, "taker": 0.0003}  # Default fees
        self.nonce = 0
        self._fetch_initial_data()
        self._start_jwt_refresh()  # Start JWT refresh thread

    def _fetch_initial_data(self):
        """Fetch initial balance, markets, and fees."""
        try:
            self.fetch_balance()
            self.markets = self._fetch_markets()
            self.fees = self._fetch_fees()
        except Exception as e:
            logger.error(f"Failed to fetch initial data: {e}")
            self.balances["USDC"] = 0
            self.available_collateral = 0

    def fetch_balance(self, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/account"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                logging.debug(f"Raw API response for balance: {response.text}")
                usdc_balance = float(data.get("total_collateral", 0))
                self.balances["USDC"] = usdc_balance
                self.available_collateral = float(data.get("free_collateral", usdc_balance))
                logging.info(f"Paradex balance updated: USDC=${usdc_balance:.2f}, Available Collateral=${self.available_collateral:.2f}")
                return usdc_balance
            except Exception as e:
                logging.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Paradex balance: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch Paradex balance after {retries} retries, using fallback")
                    self.balances["USDC"] = 0
                    self.available_collateral = 0
                    return 0
        return 0

    async def _get_jwt_token(self, paradex_config: dict, account_address: str, private_key: str) -> str:
        chain_id = int_from_bytes(paradex_config["starknet_chain_id"].encode())
        account = get_account(account_address, private_key, paradex_config)
        now = int(time.time())
        expiry = now + 24 * 60 * 60
        message = build_auth_message(chain_id, now, expiry)
        sig = account.sign_message(message)

        headers = {
            "PARADEX-STARKNET-ACCOUNT": account_address,
            "PARADEX-STARKNET-SIGNATURE": f'["{sig[0]}","{sig[1]}"]',
            "PARADEX-TIMESTAMP": str(now),
            "PARADEX-SIGNATURE-EXPIRATION": str(expiry),
        }
        url = f"{paradex_http_url}/auth"
        logging.info(f"POST {url}")
        logging.debug(f"Headers: {headers}")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                status_code = response.status
                response_data = await response.json()
                if status_code == 200:
                    logging.info(f"Success: {response_data}")
                else:
                    logging.error(f"Status Code: {status_code}, Response: {response_data}")
                return response_data.get("jwt_token", "")

    def _start_jwt_refresh(self):
        def refresh_jwt():
            while True:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    new_jwt = loop.run_until_complete(self._get_jwt_token(self.paradex_config, paradex_starknet_address, paradex_starknet_private_key))
                    loop.close()
                    if new_jwt:
                        self.jwt = new_jwt
                        self.headers = {"Authorization": f"Bearer {self.jwt}", "Content-Type": "application/json"}
                        logging.info("JWT refreshed successfully")
                    else:
                        logging.warning("Failed to refresh JWT, using existing token")
                except Exception as e:
                    logging.error(f"Error refreshing JWT: {e}")
                time.sleep(180)

        threading.Thread(target=refresh_jwt, daemon=True).start()

    def _fetch_fees(self):
        try:
            url = f"{self.base_url}/info"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Paradex fees response: {data}")
            return {
                'maker': float(data.get('makerFee', -0.00005)),
                'taker': float(data.get('takerFee', 0.0003))
            }
        except Exception as e:
            logger.error(f"Failed to fetch Paradex fees: {e}")
            return {'maker': -0.00005, 'taker': 0.0003}

    def _fetch_markets(self, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/markets"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Paradex API response for markets: {data}")
                markets = [market["symbol"].split('-')[0] for market in data.get("results", []) if market["symbol"].endswith("-USD-PERP")]
                if markets:
                    logger.info(f"Paradex markets retrieved: {markets}")
                else:
                    logger.warning("No Paradex markets found matching '-USD-PERP' criteria")
                return set(markets)
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Paradex markets: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning("Failed to fetch Paradex markets after retries, returning empty set")
                    return set()
        return set()

    def fetch_current_funding(self, market: str, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/funding/data?market={market}-USD-PERP&limit=1"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for funding {market}: {response.text}")
                if "results" in data and data["results"]:
                    rate = float(data["results"][0]["funding_rate"])
                    logger.info(f"Paradex funding rate for {market}-USD-PERP: {rate}")
                    return rate
                logger.warning(f"No funding rate data for {market}-USD-PERP")
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Paradex funding rate for {market}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to fetch Paradex funding rate for {market} after retries")
                    return None
        return None

    def fetch_current_price(self, market: str, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/markets?market={market}-USD-PERP"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for price {market}: {response.text}")
                markets = data.get("results", [])
                if markets and len(markets) > 0:
                    price = float(markets[0].get("index_price", 0))
                    if price == 0:
                        logger.warning(f"No valid index price for {market}-USD-PERP")
                        return None
                    logger.info(f"Paradex current price for {market}-USD-PERP: {price}")
                    return price
                logger.warning(f"No market data for {market}-USD-PERP")
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Paradex price for {market}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to fetch Paradex price for {market} after retries")
                    return None
        return None

    def fetch_open_interest(self, coin):
        market = f"{coin}-USD-PERP" if "-USD-PERP" not in coin else coin
        for attempt in range(3):
            try:
                headers = {"Accept": "application/json"}
                response = requests.get(
                    f"{self.base_url}/v1/markets/summary",
                    params={"market": market},
                    headers=headers,
                    timeout=5
                )
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Paradex open interest response for {market}: {data}")
                if data and "results" in data and len(data["results"]) > 0:
                    open_interest = float(data["results"][0].get("open_interest", 0))
                    return open_interest
                logger.warning(f"No open interest data for {market} in Paradex response")
                return 0
            except requests.RequestException as e:
                logger.error(f"Attempt {attempt + 1}/3 - Failed to fetch Paradex open interest for {market}: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        logger.error(f"Failed to fetch Paradex open interest for {market} after retries")
        return 0

    def get_fees(self):
        return self.fees

    def check_balance(self, trade_size, leverage=1, fee_rate=0.001):
        effective_balance = min(self.balances["USDC"], self.available_collateral)
        required_balance = trade_size * leverage * (1 + fee_rate * 2)
        logger.debug(f"Paradex balance check: Effective=${effective_balance:.2f}, Required=${required_balance:.2f}")
        return effective_balance

    def check_available_collateral(self):
        return self.available_collateral

    def check_liquidity(self, market: str, order_size: float, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/orderbook?market={market}-USD-PERP&depth=10"
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for liquidity {market}: {response.text}")
                if "bids" in data and "asks" in data:
                    total_liquidity = sum(float(bid[1]) * float(bid[0]) for bid in data["bids"]) + sum(float(ask[1]) * float(ask[0]) for ask in data["asks"])
                    logger.debug(f"Paradex liquidity for {market}-USD-PERP: ${total_liquidity:.2f}, Order Size: ${order_size}")
                    return total_liquidity
                logger.warning(f"No orderbook data for {market}-USD-PERP")
                return 0
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to check Paradex liquidity for {market}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to check Paradex liquidity for {market} after retries")
                    return 0
        return 0

    def place_limit_order(self, market: str, side: str, price: float, size: float, reduce_only: bool = False, retries=3):
        for attempt in range(retries):
            try:
                self.nonce += 1
                payload = {
                    "market": f"{market}-USD-PERP",
                    "side": side.upper(),
                    "price": str(price),
                    "size": str(size),
                    "type": "limit",
                    "reduce_only": reduce_only,
                    "nonce": self.nonce,
                    "timestamp": int(time.time() * 1000)
                }
                url = f"{self.base_url}/orders"
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for order placement {market}: {response.text}")
                if "order_id" in data:
                    order_id = data["order_id"]
                    logger.info(f"Paradex limit order placed for {market}-USD-PERP: {side} at ${price}, size {size}, order_id={order_id}")
                    return order_id
                logger.warning(f"Order not placed for {market}-USD-PERP: {data}")
                if attempt < retries - 1:
                    price = price * (1 + 0.002 if side.upper() == "BUY" else 1 - 0.002)
                    logger.info(f"Retrying with adjusted price ${price}")
                    time.sleep(30)
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to place Paradex limit order for {market}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to place Paradex limit order for {market} after retries")
                    return None
        return None

    def cancel_order(self, order_id: str, retries=3):
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/orders/{order_id}"
                response = requests.delete(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                logger.info(f"Paradex order {order_id} cancelled successfully")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to cancel Paradex order {order_id}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to cancel Paradex order {order_id} after retries")
                    return False
        return False

class HyperliquidClient:
    def __init__(self):
        self.api_key = os.getenv("HYPERLIQUID_API_KEY")
        self.api_secret = os.getenv("HYPERLIQUID_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise ValueError("HYPERLIQUID_API_KEY and HYPERLIQUID_API_SECRET must be set in .env file")
        try:
            self.ws_manager = WebsocketManager(base_url="wss://api.hyperliquid.xyz/ws")
        except TypeError as e:
            logger.error(f"Failed to instantiate WebsocketManager: {str(e)}")
            raise
        self.base_url = "https://api.hyperliquid.xyz"
        self.balances = {"USDC": 0}
        self.available_collateral = 0
        self.markets = set()
        self.fees = {"maker": 0.0001, "taker": 0.00035}
        self.asset_indices = {}
        self.funding_rates = {}
        self._fetch_initial_data()

    def _fetch_initial_data(self):
        """Fetch initial balance, markets, fees, and asset indices."""
        try:
            self.fetch_balance()
            self.markets = self._fetch_markets()
            self.fees = self._fetch_fees()
            self.asset_indices = self._fetch_asset_indices()
            self.start_websocket([coin + "-PERP" if "-PERP" not in coin else coin for coin in self.markets])
        except Exception as e:
            logger.error(f"Failed to fetch initial data: {e}")
            self.balances["USDC"] = 0
            self.available_collateral = 0

    def start_websocket(self, coins):
        logger.info("Starting Hyperliquid WebSocket...")
        try:
            self.ws_manager.subscribe("fundingRate", coins, self.on_message)
            logger.info(f"Hyperliquid WebSocket subscribed to: {coins}")
        except Exception as e:
            logger.error(f"Failed to start Hyperliquid WebSocket: {e}")
            logger.info("Continuing with HTTP fallback for funding rates")

    def _fetch_asset_indices(self):
        try:
            payload = {"type": "meta"}
            response = requests.post(f"{self.base_url}/info", json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Hyperliquid meta response: {data}")
            return {coin["name"]: idx for idx, coin in enumerate(data.get("universe", []))}
        except Exception as e:
            logger.error(f"Failed to fetch Hyperliquid asset indices: {e}")
            return {}

    def _fetch_markets(self):
        try:
            payload = {"type": "meta"}
            response = requests.post(f"{self.base_url}/info", json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Hyperliquid meta response: {data}")
            markets = [coin["name"] for coin in data.get("universe", []) if not coin.get("isDelisted", False)]
            if markets:
                logger.info(f"Hyperliquid markets retrieved: {markets}")
            else:
                logger.warning("No markets found in Hyperliquid universe")
            return set(markets)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to fetch Hyperliquid markets: {e}")
            return set()
        except Exception as e:
            logger.error(f"Unexpected error fetching Hyperliquid markets: {e}")
            return set()

    def _fetch_fees(self):
        try:
            payload = {"type": "fees"}
            response = requests.post(f"{self.base_url}/info", json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Hyperliquid fees response: {data}")
            return {
                'maker': float(data.get('makerFee', 0.0001)),
                'taker': float(data.get('takerFee', 0.00035))
            }
        except Exception as e:
            logger.error(f"Failed to fetch Hyperliquid fees: {e}")
            return {'maker': 0.0001, 'taker': 0.00035}

    def get_fees(self):
        return self.fees

    def fetch_balance(self, retries=3):
        for attempt in range(retries):
            try:
                user_address = os.getenv("HYPERLIQUID_WALLET_ADDRESS", "0x0000000000000000000000000000000000000000")
                if not user_address.startswith("0x") or len(user_address) != 42 or not all(c in "0123456789abcdefABCDEF" for c in user_address[2:]):
                    raise ValueError(f"Invalid HYPERLIQUID_WALLET_ADDRESS: {user_address}")

                payload = {"type": "spotClearinghouseState", "user": user_address}
                message = f"POST /info {json.dumps(payload, separators=(',', ':'))}".encode()
                timestamp = int(time.time() * 1000)
                nonce = int(time.time() * 1000)
                signature = hmac.new(self.api_secret.encode(), message, hashlib.sha256).hexdigest()
                headers = {
                    "Content-Type": "application/json",
                    "X-API-Key": self.api_key,
                    "X-Signature": signature,
                    "X-Timestamp": str(timestamp),
                    "X-Nonce": str(nonce)
                }
                response = requests.post(f"{self.base_url}/info", headers=headers, json=payload, timeout=10)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for balance: {data}")
                usdc_balance = 0.0
                for token in data.get("balances", []):
                    if token.get("coin") == "USDC":
                        usdc_balance = float(token.get("total", 0.0))
                        break
                self.balances["USDC"] = usdc_balance
                self.available_collateral = usdc_balance
                logger.info(f"Hyperliquid balance updated: USDC=${usdc_balance:.2f}")
                return usdc_balance
            except requests.exceptions.HTTPError as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Hyperliquid balance: {e}, Response: {response.text if 'response' in locals() else 'No response'}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.error("Failed to fetch Hyperliquid balance after retries, using fallback")
                    self.balances["USDC"] = 0
                    self.available_collateral = 0
                    return 0
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Unexpected error fetching Hyperliquid balance: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.error("Failed to fetch Hyperliquid balance after retries, using fallback")
                    self.balances["USDC"] = 0
                    self.available_collateral = 0
                    return 0
        return 0

    def _sign_request(self, action: dict):
        message = json.dumps(action, separators=(',', ':'))
        encoded_message = encode_defunct(text=message)
        # Note: eth_account is not initialized; this needs a valid account
        # For now, we'll assume a mock implementation or require initialization
        raise NotImplementedError("eth_account must be initialized for signing")

    def on_message(self, message):
        try:
            data = json.loads(message)
            if data.get("type") == "fundingRate" and "coin" in data.get("data", {}):
                coin = data["data"]["coin"]
                self.funding_rates[coin] = float(data["data"]["fundingRate"])
                logger.info(f"Hyperliquid funding rate updated for {coin}: {self.funding_rates[coin]}")
        except Exception as e:
            logger.error(f"Error processing Hyperliquid message: {e}, Message: {message}")

    def fetch_current_funding_http(self, coin: str, retries=3):
        for attempt in range(retries):
            try:
                payload = {
                    "type": "fundingHistory",
                    "coin": coin,
                    "startTime": int(time.time() * 1000) - 3600000,
                    "endTime": int(time.time() * 1000)
                }
                response = requests.post(f"{self.base_url}/info", json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for funding history {coin}: {data}")
                if data and isinstance(data, list) and len(data) > 0:
                    rate = float(data[-1].get("fundingRate", 0))
                    if rate != 0:
                        logger.info(f"Hyperliquid HTTP funding rate for {coin}: {rate}")
                        return rate
                    logger.warning(f"No valid funding rate found for {coin}")
                else:
                    logger.warning(f"No funding rate data for {coin}")
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Hyperliquid funding rate for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to fetch Hyperliquid funding rate for {coin} after retries")
                    return None
        return None

    def fetch_predicted_funding(self, coin):
        coin = coin + "-PERP" if "-PERP" not in coin else coin
        for attempt in range(3):
            try:
                response = requests.post(
                    f"{self.base_url}/info",
                    headers={"Content-Type": "application/json"},
                    json={"type": "predictedFundings"},
                    timeout=5
                )
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Predicted funding response for {coin}: {data}")
                for coin_data in data:
                    coin_name = coin_data[0]
                    venues = coin_data[1]
                    if coin_name == coin or coin_name == coin.replace("-PERP", ""):
                        for venue in venues:
                            if venue[0] == "HlPerp":
                                predicted_rate = float(venue[1]["fundingRate"])
                                next_funding_time = int(venue[1]["nextFundingTime"])
                                return predicted_rate, next_funding_time
                logger.warning(f"No predicted funding data for {coin}")
                return self.get_current_funding(coin), 0
            except requests.RequestException as e:
                logger.error(f"Attempt {attempt+1}/3 - Failed to fetch predicted funding for {coin}: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        logger.error(f"Failed to fetch predicted funding for {coin} after retries")
        return self.get_current_funding(coin), 0

    def fetch_current_price(self, coin: str, retries=3):
        for attempt in range(retries):
            try:
                payload = {"type": "metaAndAssetCtxs"}
                response = requests.post(f"{self.base_url}/info", json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for price {coin}: {data}")
                if len(data) >= 2:
                    asset_ctxs = data[1]
                    index = self.asset_indices.get(coin)
                    if index is not None and str(index) in asset_ctxs:
                        price = float(asset_ctxs[str(index)].get("indexPx", 0))
                        if price != 0:
                            logger.info(f"Hyperliquid current price for {coin}: {price}")
                            return price
                        logger.warning(f"No valid index price for {coin}")
                else:
                    logger.warning(f"No price data for {coin}")
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Hyperliquid price for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to fetch Hyperliquid price for {coin} after retries")
                    return None
        return None

    def fetch_open_interest(self, coin: str, retries=3):
        for attempt in range(retries):
            try:
                payload = {"type": "openInterest", "coin": coin}
                response = requests.post(f"{self.base_url}/info", json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for open interest {coin}: {data}")
                oi = float(data.get("openInterest", 0))
                logger.info(f"Open interest for {coin} on Hyperliquid: {oi}")
                return oi
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to fetch Hyperliquid open interest for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to fetch Hyperliquid open interest for {coin} after retries")
                    return 0
        return 0

    def get_current_funding(self, coin: str):
        rate = self.funding_rates.get(coin)
        if rate is None:
            logger.warning(f"No WebSocket funding rate for {coin}, using HTTP fallback...")
            rate = self.fetch_current_funding_http(coin)
        return rate

    def check_balance(self, trade_size, leverage=1, fee_rate=0.001):
        effective_balance = min(self.balances["USDC"], self.available_collateral)
        required_balance = trade_size * leverage * (1 + fee_rate * 2)
        logger.debug(f"Hyperliquid balance check: Effective=${effective_balance:.2f}, Required=${required_balance:.2f}")
        return effective_balance

    def check_available_collateral(self):
        return self.available_collateral

    def check_liquidity(self, coin: str, order_size: float, retries=3):
        for attempt in range(retries):
            try:
                payload = {"type": "l2Book", "coin": coin}
                response = requests.post(f"{self.base_url}/info", json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for liquidity {coin}: {data}")
                if "levels" in data:
                    bids = data["levels"][0]
                    asks = data["levels"][1]
                    total_liquidity = sum(float(bid["px"]) * float(bid["sz"]) for bid in bids) + sum(float(ask["px"]) * float(ask["sz"]) for ask in asks)
                    logger.debug(f"Hyperliquid liquidity for {coin}: ${total_liquidity:.2f}, Order Size: ${order_size}")
                    return total_liquidity
                logger.warning(f"No orderbook data for {coin}")
                return 0
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to check Hyperliquid liquidity for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to check Hyperliquid liquidity for {coin} after retries")
                    return 0
        return 0

    def place_limit_order(self, coin: str, side: str, price: float, size: float, reduce_only: bool = False, retries=3):
        for attempt in range(retries):
            try:
                asset_index = self.asset_indices.get(coin)
                if asset_index is None:
                    logger.error(f"No asset index found for {coin}")
                    return None
                action = {
                    "type": "order",
                    "asset": asset_index,
                    "isBuy": side.lower() == "buy",
                    "limitPx": str(price),
                    "sz": str(size),
                    "reduceOnly": reduce_only,
                    "orderType": {"limit": {"tif": "Gtc"}},
                    "nonce": int(time.time() * 1000)
                }
                # Placeholder for signing (requires eth_account initialization)
                signature = {"r": "0x0", "s": "0x0", "v": 0}  # Mock signature
                payload = {"action": action, "signature": signature}
                response = requests.post(f"{self.base_url}/exchange", json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                logger.debug(f"Raw API response for order placement {coin}: {data}")
                if "response" in data and "order" in data["response"]:
                    order_id = data["response"]["order"].get("oid")
                    if order_id:
                        logger.info(f"Hyperliquid limit order placed for {coin}: {side} at ${price}, size {size}, order_id={order_id}")
                        return order_id
                logger.warning(f"Order not placed for {coin}: {data}")
                if attempt < retries - 1:
                    price = price * (1 + 0.002 if side.lower() == "buy" else 1 - 0.002)
                    logger.info(f"Retrying with adjusted price ${price}")
                    time.sleep(30)
                return None
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to place Hyperliquid limit order for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to place Hyperliquid limit order for {coin} after retries")
                    return None
        return None

    def cancel_order(self, coin: str, order_id: int, retries=3):
        for attempt in range(retries):
            try:
                asset_index = self.asset_indices.get(coin)
                if asset_index is None:
                    logger.error(f"No asset index found for {coin}")
                    return False
                action = {
                    "type": "cancel",
                    "asset": asset_index,
                    "oid": order_id,
                    "nonce": int(time.time() * 1000)
                }
                signature = {"r": "0x0", "s": "0x0", "v": 0}  # Mock signature
                payload = {"action": action, "signature": signature}
                response = requests.post(f"{self.base_url}/exchange", json=payload, timeout=30)
                response.raise_for_status()
                logger.info(f"Hyperliquid order {order_id} for {coin} cancelled successfully")
                return True
            except Exception as e:
                logger.error(f"Attempt {attempt+1}/{retries} - Failed to cancel Hyperliquid order {order_id} for {coin}: {e}")
                time.sleep(5)
                if attempt == retries - 1:
                    logger.warning(f"Failed to cancel Hyperliquid order {order_id} for {coin} after retries")
                    return False
        return False

class LiveArbitrageBot:
    def __init__(self, paradex_client, hyperliquid_client, trade_size=10, leverage=1, min_diff_threshold=0.001, max_trades_per_day=5, max_loss_threshold=500, root=None):
        self.paradex = paradex_client
        self.hyperliquid = hyperliquid_client
        self.trade_size = trade_size
        self.leverage = leverage
        self.min_diff_threshold = min_diff_threshold
        self.max_trades_per_day = max_trades_per_day
        self.max_loss_threshold = max_loss_threshold
        self.running = False
        self.trade_count = 0
        self.active_trades = {}
        self.last_day = None
        self.common_coins = ["BTC-USD-PERP", "ETH-USD-PERP", "EIGEN-USD-PERP", "MEW-USD-PERP"]
        self.coins = self.common_coins
        self.symbols = [coin.split("-")[0] for coin in self.coins]
        self.update_queue = queue.Queue()
        self.root = root if root is not None else tk.Tk()
        self.last_balance_print = time.time()
        self.setup_ui()
        self.notional = self.trade_size * self.leverage
        self.risk_threshold = 0.30
        self._initialize_fees()

    def _initialize_fees(self):
        """Initialize fees from clients to avoid attribute errors."""
        self.paradex_fees = self.paradex.get_fees()
        self.hyperliquid_fees = self.hyperliquid.get_fees()
        self.total_cost_base = (self.paradex_fees["maker"] + self.hyperliquid_fees["maker"]) * self.notional

    def setup_ui(self):
        self.root.title("Arbitrage Bot - Live Mode")
        self.root.geometry("1200x800")
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        main_frame = ttk.Frame(notebook)
        notebook.add(main_frame, text="Main")
        param_frame = ttk.LabelFrame(main_frame, text="Parameters")
        param_frame.pack(fill=tk.X, padx=5, pady=5)
        tk.Label(param_frame, text="Trade Size ($):").grid(row=0, column=0, padx=5, pady=5)
        self.trade_size_entry = tk.Entry(param_frame)
        self.trade_size_entry.insert(0, str(self.trade_size))
        self.trade_size_entry.grid(row=0, column=1)
        tk.Label(param_frame, text="Leverage (x):").grid(row=0, column=2, padx=5, pady=5)
        self.leverage_entry = tk.Entry(param_frame)
        self.leverage_entry.insert(0, str(self.leverage))
        self.leverage_entry.grid(row=0, column=3)
        tk.Label(param_frame, text="Min Diff Threshold (%):").grid(row=1, column=0, padx=5, pady=5)
        self.min_diff_entry = tk.Entry(param_frame)
        self.min_diff_entry.insert(0, str(self.min_diff_threshold * 100))
        self.min_diff_entry.grid(row=1, column=1)
        tk.Label(param_frame, text="Max Trades/Day:").grid(row=1, column=2, padx=5, pady=5)
        self.max_trades_entry = tk.Entry(param_frame)
        self.max_trades_entry.insert(0, str(self.max_trades_per_day))
        self.max_trades_entry.grid(row=1, column=3)
        tk.Label(param_frame, text="Max Loss ($):").grid(row=2, column=0, padx=5, pady=5)
        self.max_loss_entry = tk.Entry(param_frame)
        self.max_loss_entry.insert(0, str(self.max_loss_threshold))
        self.max_loss_entry.grid(row=2, column=1)
        ttk.Button(param_frame, text="Apply", command=self.apply_params).grid(row=3, column=0, columnspan=4, pady=5)
        balance_frame = ttk.LabelFrame(main_frame, text="Balances & PNL")
        balance_frame.pack(fill=tk.X, padx=5, pady=5)
        self.paradex_balance_label = tk.Label(balance_frame, text=f"Paradex: ${self.paradex.balances.get('USDC', 0.0):.2f}")
        self.paradex_balance_label.pack(side=tk.LEFT, padx=5, pady=5)
        self.hyperliquid_balance_label = tk.Label(balance_frame, text=f"Hyperliquid: ${self.hyperliquid.balances.get('USDC', 0.0):.2f}")
        self.hyperliquid_balance_label.pack(side=tk.LEFT, padx=5, pady=5)
        self.total_pnl_label = tk.Label(balance_frame, text=f"Total PNL (Balances): $0.00")
        self.total_pnl_label.pack(side=tk.LEFT, padx=5, pady=5)
        rates_frame = ttk.LabelFrame(main_frame, text="Funding Rates")
        rates_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.tree = ttk.Treeview(rates_frame, columns=("Coin", "Paradex Rate", "Hyperliquid Rate", "Predicted HL Rate", "Diff", "PNL", "Risk", "Liquidity", "OI Paradex", "OI Hyperliquid"), show="headings")
        self.tree.heading("Coin", text="Coin", command=lambda: self.sort_column("Coin", False))
        self.tree.heading("Paradex Rate", text="Paradex Rate", command=lambda: self.sort_column("Paradex Rate", False))
        self.tree.heading("Hyperliquid Rate", text="Hyperliquid Rate", command=lambda: self.sort_column("Hyperliquid Rate", False))
        self.tree.heading("Predicted HL Rate", text="Predicted HL Rate", command=lambda: self.sort_column("Predicted HL Rate", False))
        self.tree.heading("Diff", text="Diff", command=lambda: self.sort_column("Diff", False))
        self.tree.heading("PNL", text="PNL Net", command=lambda: self.sort_column("PNL", False))
        self.tree.heading("Risk", text="Risk", command=lambda: self.sort_column("Risk", False))
        self.tree.heading("Liquidity", text="Liquidity", command=lambda: self.sort_column("Liquidity", False))
        self.tree.heading("OI Paradex", text="OI Paradex", command=lambda: self.sort_column("OI Paradex", False))
        self.tree.heading("OI Hyperliquid", text="OI Hyperliquid", command=lambda: self.sort_column("OI Hyperliquid", False))
        self.tree.pack(fill=tk.BOTH, expand=True)
        for col in self.tree["columns"]:
            self.tree.column(col, width=100, stretch=tk.YES)
        self.tree.tag_configure('gray_text', foreground='gray')
        self.tree.tag_configure('green_text', foreground='green')
        positions_frame = ttk.Frame(notebook)
        notebook.add(positions_frame, text="Positions")
        active_positions_frame = ttk.LabelFrame(positions_frame, text="Active Positions")
        active_positions_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.positions_tree = ttk.Treeview(active_positions_frame, columns=("Coin", "Side_Paradex", "Side_Hyperliquid", "FR_Paradex", "FR_Hyperliquid", "Predicted HL Rate", "PNL_RealTime", "Fees_Paradex", "Fees_Hyperliquid", "Time_Remaining", "Order_Status", "OI Paradex", "OI Hyperliquid", "Action"), show="headings")
        self.positions_tree.heading("Coin", text="Coin", command=lambda: self.sort_column_positions("Coin", False))
        self.positions_tree.heading("Side_Paradex", text="Side Paradex", command=lambda: self.sort_column_positions("Side_Paradex", False))
        self.positions_tree.heading("Side_Hyperliquid", text="Side Hyperliquid", command=lambda: self.sort_column_positions("Side_Hyperliquid", False))
        self.positions_tree.heading("FR_Paradex", text="FR Paradex", command=lambda: self.sort_column_positions("FR_Paradex", False))
        self.positions_tree.heading("FR_Hyperliquid", text="FR Hyperliquid", command=lambda: self.sort_column_positions("FR_Hyperliquid", False))
        self.positions_tree.heading("Predicted HL Rate", text="Predicted HL Rate", command=lambda: self.sort_column_positions("Predicted HL Rate", False))
        self.positions_tree.heading("PNL_RealTime", text="PNL Real-Time", command=lambda: self.sort_column_positions("PNL_RealTime", False))
        self.positions_tree.heading("Fees_Paradex", text="Fees Paradex", command=lambda: self.sort_column_positions("Fees_Paradex", False))
        self.positions_tree.heading("Fees_Hyperliquid", text="Fees Hyperliquid", command=lambda: self.sort_column_positions("Fees_Hyperliquid", False))
        self.positions_tree.heading("Time_Remaining", text="Time Remaining (h:m:s)", command=lambda: self.sort_column_positions("Time_Remaining", False))
        self.positions_tree.heading("Order_Status", text="Order Status", command=lambda: self.sort_column_positions("Order_Status", False))
        self.positions_tree.heading("OI Paradex", text="OI Paradex", command=lambda: self.sort_column_positions("OI Paradex", False))
        self.positions_tree.heading("OI Hyperliquid", text="OI Hyperliquid", command=lambda: self.sort_column_positions("OI Hyperliquid", False))
        self.positions_tree.heading("Action", text="Action", command=lambda: self.sort_column_positions("Action", False))
        self.positions_tree.pack(fill=tk.BOTH, expand=True)
        for col in self.positions_tree["columns"]:
            self.positions_tree.column(col, width=100, stretch=tk.YES)
        trade_history_frame = ttk.LabelFrame(positions_frame, text="Trade History")
        trade_history_frame.pack(fill=tk.X, padx=5, pady=5)
        self.trade_log = scrolledtext.ScrolledText(trade_history_frame, wrap=tk.WORD, height=5)
        self.trade_log.pack(fill=tk.X, expand=True, padx=5, pady=5)
        log_frame = ttk.Frame(notebook)
        notebook.add(log_frame, text="Detailed Logs")
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        class ScrolledTextHandler(logging.Handler):
            def __init__(self, widget):
                super().__init__()
                self.widget = widget

            def emit(self, record):
                msg = self.format(record)
                self.widget.insert(tk.END, msg + "\n")
                self.widget.see(tk.END)

        logging.getLogger().handlers = []
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        gui_handler = ScrolledTextHandler(self.log_text)
        gui_handler.setLevel(logging.DEBUG)
        logger.addHandler(console_handler)
        logger.addHandler(gui_handler)
        logger.setLevel(logging.DEBUG)

        control_frame = ttk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        self.status_label = tk.Label(control_frame, text="Status: Stopped")
        self.status_label.pack(side=tk.LEFT)
        ttk.Button(control_frame, text="Start", command=self.start_bot).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Stop", command=self.stop_bot).pack(side=tk.LEFT, padx=5)
        self.alert_button = ttk.Button(control_frame, text="Show Alerts", command=self.show_alerts)
        self.alert_button.pack(side=tk.LEFT, padx=5)

        self.update_timer = None
        self.start_real_time_update()

    def check_balances(self):
        min_trade_size = 5.0
        fee_rate = 0.001
        required_balance = max(self.trade_size * self.leverage, min_trade_size) * (1 + fee_rate * 2)
        paradex_balance = self.paradex.check_balance(self.trade_size, self.leverage, fee_rate)
        hyperliquid_balance = self.hyperliquid.check_balance(self.trade_size, self.leverage, fee_rate)
        logger.debug(f"Balance validation: Required=${required_balance:.2f}, Paradex=${paradex_balance:.2f}, Hyperliquid=${hyperliquid_balance:.2f}")
        return paradex_balance >= required_balance and hyperliquid_balance >= required_balance

    def update_parameters(self, trade_size=None, leverage=None, min_diff_threshold=None, max_trades_per_day=None, max_loss_threshold=None):
        if trade_size is not None:
            self.trade_size = float(trade_size)
        if leverage is not None:
            self.leverage = float(leverage)
        if min_diff_threshold is not None:
            self.min_diff_threshold = float(min_diff_threshold) / 100
        if max_trades_per_day is not None:
            self.max_trades_per_day = int(max_trades_per_day)
        if max_loss_threshold is not None:
            self.max_loss_threshold = float(max_loss_threshold)
        self.notional = self.trade_size * self.leverage
        self._initialize_fees()  # Reinitialize fees with new trade size
        logger.info(f"Updated parameters: trade_size={self.trade_size}, leverage={self.leverage}, min_diff_threshold={self.min_diff_threshold}, max_trades_per_day={self.max_trades_per_day}, max_loss_threshold={self.max_loss_threshold}")

    def apply_params(self):
        try:
            new_trade_size = float(self.trade_size_entry.get() or self.trade_size)
            new_leverage = float(self.leverage_entry.get() or self.leverage)
            new_min_diff_threshold = float(self.min_diff_entry.get() or self.min_diff_threshold * 100) / 100
            new_max_trades_per_day = int(self.max_trades_entry.get() or self.max_trades_per_day)
            new_max_loss_threshold = float(self.max_loss_entry.get() or self.max_loss_threshold)
            self.update_parameters(
                trade_size=new_trade_size,
                leverage=new_leverage,
                min_diff_threshold=new_min_diff_threshold,
                max_trades_per_day=new_max_trades_per_day,
                max_loss_threshold=new_max_loss_threshold
            )
            if not self.check_balances():
                raise ValueError("Insufficient balance for new parameters")
            logger.info(f"Applied parameters: trade_size={self.trade_size}, leverage={self.leverage}, required_balance={max(self.trade_size * self.leverage, 5.0) * (1 + 0.002):.2f}")
        except (ValueError, KeyError) as e:
            logger.error(f"Error applying parameters: {e}")
            messagebox.showerror("Error", f"Invalid input: {e}. Please enter valid numbers.")
        except Exception as e:
            logger.error(f"Unexpected error in apply_params: {e}")
            messagebox.showerror("Error", f"Unexpected error: {e}")

    def calculate_volatility(self, coin, period_minutes=60):
        try:
            start_time = int(time.time() * 1000) - (period_minutes * 60 * 1000)
            end_time = int(time.time() * 1000)
            paradex_url = f"{self.paradex.base_url}/funding/data?market={coin}-USD-PERP&startTime={start_time}&endTime={end_time}&limit=60"
            hyperliquid_payload = {"type": "fundingHistory", "coin": coin, "startTime": start_time, "endTime": end_time}
            paradex_response = requests.get(paradex_url, headers=self.paradex.headers, timeout=30)
            hyperliquid_response = requests.post(f"{self.hyperliquid.base_url}/info", json=hyperliquid_payload, timeout=30)
            paradex_data = paradex_response.json().get("results", [])
            hyperliquid_data = hyperliquid_response.json()
            rates = [float(r["funding_rate"]) for r in paradex_data] + [float(h.get("fundingRate", 0)) for h in hyperliquid_data if h.get("fundingRate")]
            if len(rates) < 2:
                return 0.0
            mean = sum(rates) / len(rates)
            variance = sum((x - mean) ** 2 for x in rates) / len(rates)
            volatility = (variance ** 0.5) * 100
            logger.info(f"Volatility for {coin} over {period_minutes} minutes: {volatility:.4f}%")
            return volatility
        except Exception as e:
            logger.error(f"Error calculating volatility for {coin}: {e}")
            return 0.0

    def calculate_risk(self, coin, diff):
        try:
            oi_paradex = self.paradex.fetch_open_interest(coin)
            oi_hyperliquid = self.hyperliquid.fetch_open_interest(coin)
            oi_ratio = oi_hyperliquid / oi_paradex if oi_paradex > 0 else 1.0
            base_risk = abs(diff) * 100
            oi_risk = abs(oi_ratio - 1) * 10
            volatility = self.calculate_volatility(coin)
            total_risk = min(100, base_risk + oi_risk + (volatility / 2))
            logger.info(f"Risk for {coin}: {total_risk:.2f}% (Diff: {base_risk:.2f}%, OI: {oi_risk:.2f}%, Vol: {volatility:.2f}%)")
            return total_risk
        except Exception as e:
            logger.error(f"Error calculating risk for {coin}: {e}")
            return 100.0

    def predict_funding_rate_with_oi(self, coin, fr_current, fr_predicted, oi_paradex, oi_hyperliquid):
        if oi_paradex == 0 or oi_hyperliquid == 0:
            logger.info(f"No open interest data for {coin} on one or both platforms. Using predicted rate if available.")
            return fr_predicted if fr_predicted else fr_current
        oi_ratio = oi_hyperliquid / oi_paradex if oi_paradex > 0 else 1.0
        if oi_ratio > 2.0:
            predicted_rate = max(fr_current, fr_predicted or 0) * 1.1
        elif oi_ratio < 0.5:
            predicted_rate = min(fr_current, fr_predicted or 0) * 0.9
        else:
            predicted_rate = fr_predicted if fr_predicted else fr_current
        logger.info(f"Predicted funding rate for {coin} adjusted by OI: {predicted_rate}")
        return predicted_rate

    def start_bot(self):
        logger.debug(f"Start button clicked, current running state: {self.running}")
        if not self.running:
            try:
                self.running = True
                self.status_label.config(text="Status: Running")
                logger.info("Attempting to start Hyperliquid WebSocket...")
                self.hyperliquid.start_websocket(self.coins)
                logger.info(f"WebSocket started with coins: {self.coins}")
                logger.info(f"Starting bot with coins: {self.coins}")
                bot_thread = threading.Thread(target=self.run, daemon=True)
                bot_thread.start()
                logger.info("Bot thread started successfully")
                # Ensure queue processing starts immediately and continues
                self.process_queue()  # Initial call
                self.root.after(100, self.process_queue)  # Schedule continuous processing
            except Exception as e:
                logger.error(f"Failed to start bot: {e}", exc_info=True)
                self.running = False
                self.status_label.config(text="Status: Failed to Start")
                messagebox.showerror("Start Error", f"Failed to start bot: {e}")

    def stop_bot(self):
        self.running = False
        self.status_label.config(text="Status: Stopped")
        if self.update_timer:
            self.root.after_cancel(self.update_timer)
        logger.info("Bot stopped")

    def start_real_time_update(self):
        self.update_ui()
        self.update_timer = self.root.after(1000, self.start_real_time_update)

    def run(self):
        """Main bot loop for arbitrage trading."""
        logger.info("Bot loop started")
        while self.running:
            try:
                logger.debug("Entering bot loop iteration")
                current_day = time.strftime("%Y-%m-%d", time.gmtime())
                if not hasattr(self, 'last_day') or self.last_day != current_day:
                    self.trade_count = 0
                    self.active_trades = {}
                    self.last_day = current_day
                    logger.info(f"New day detected, reset trades. Coins: {self.common_coins}")

                if self.trade_count >= self.max_trades_per_day:
                    logger.warning("Max trades per day reached. Waiting until tomorrow...")
                    time.sleep(3600)
                    continue

                logger.debug("Fetching balances...")
                self.paradex.fetch_balance()
                self.hyperliquid.fetch_balance()
                logger.debug("Calculating position PNL...")
                total_pnl = self.calculate_position_pnl()
                if total_pnl < -self.max_loss_threshold:
                    logger.error(f"Max loss threshold exceeded: Total PNL ${total_pnl:.2f}")
                    self.running = False
                    self.root.after(0, lambda: messagebox.showerror("Critical Error", f"Max loss reached: ${total_pnl:.2f}"))
                    break

                logger.debug(f"Checking funding rates for coins: {self.common_coins}")
                funding_diffs = {}
                processed_coins = set()
                for symbol, coin in zip(self.symbols, self.common_coins):
                    if coin in processed_coins:
                        continue
                    try:
                        required_balance = max(self.trade_size * self.leverage, 5.0) * (1 + 0.002)
                        if not (self.paradex.check_balance(self.trade_size) >= required_balance and self.hyperliquid.check_balance(self.trade_size) >= required_balance):
                            logger.warning(f"Insufficient balance for {coin}")
                            continue
                        fr_paradex = self.paradex.fetch_current_funding(coin)
                        fr_hyperliquid = self.hyperliquid.get_current_funding(coin)
                        if fr_paradex is None or fr_hyperliquid is None:
                            logger.warning(f"Funding rates unavailable for {coin}")
                            continue
                        predicted_hl_rate, next_funding_time = self.hyperliquid.fetch_predicted_funding(coin) or (0, 0)
                        oi_paradex = self.paradex.fetch_open_interest(coin)
                        oi_hyperliquid = self.hyperliquid.fetch_open_interest(coin)
                        if oi_paradex == 0 and oi_hyperliquid == 0:
                            logger.warning(f"No open interest for {coin}")
                            continue
                        diff = fr_paradex - fr_hyperliquid
                        predicted_rate = self.predict_funding_rate_with_oi(coin, fr_hyperliquid, predicted_hl_rate, oi_paradex, oi_hyperliquid)
                        if abs(diff) < self.min_diff_threshold:
                            logger.debug(f"Diff {diff*100:.4f}% below threshold for {coin}")
                            continue
                        funding_diffs[coin] = {
                            "diff": diff,
                            "symbol": symbol,
                            "fr_paradex": fr_paradex,
                            "fr_hyperliquid": fr_hyperliquid,
                            "predicted_hl_rate": predicted_hl_rate,
                            "oi_paradex": oi_paradex,
                            "oi_hyperliquid": oi_hyperliquid,
                            "next_funding_time": next_funding_time
                        }
                        logger.debug(f"Funding rates for {coin}: Paradex={fr_paradex*100:.4f}%, Hyperliquid={fr_hyperliquid*100:.4f}%, Diff={diff*100:.4f}%")
                    except Exception as e:
                        logger.error(f"Error processing {coin}: {e}")
                        time.sleep(30)
                        continue
                    processed_coins.add(coin)

                if funding_diffs:
                    self.update_queue.put(("update_ui", funding_diffs, None))
                    best_coin = max(funding_diffs.items(), key=lambda x: abs(x[1]["diff"]))[0]
                    best_diff = funding_diffs[best_coin]["diff"]
                    if abs(best_diff) >= self.min_diff_threshold and best_coin not in self.active_trades:
                        risk = self.calculate_risk(best_coin, best_diff)
                        if risk <= self.risk_threshold:
                            liquidity = self.check_liquidity_paradex_hyperliquid(best_coin, self.trade_size)
                            if liquidity >= self.trade_size * 1.5:
                                current_paradex_price = self.paradex.fetch_current_price(best_coin)
                                current_hyperliquid_price = self.hyperliquid.fetch_current_price(best_coin)
                                if current_paradex_price and current_hyperliquid_price:
                                    price_buffer = 0.001
                                    side_paradex = "short" if best_diff > 0 else "long"
                                    side_hyperliquid = "long" if best_diff > 0 else "short"
                                    paradex_limit_price = current_paradex_price * (1 - price_buffer if side_paradex == "short" else 1 + price_buffer)
                                    hyperliquid_limit_price = current_hyperliquid_price * (1 - price_buffer if side_hyperliquid == "short" else 1 + price_buffer)
                                    size = self.trade_size / current_paradex_price
                                    paradex_order_id = self.paradex.place_limit_order(best_coin, side_paradex, paradex_limit_price, size)
                                    hyperliquid_order_id = self.hyperliquid.place_limit_order(best_coin, side_hyperliquid, hyperliquid_limit_price, size)
                                    if paradex_order_id and hyperliquid_order_id:
                                        self.trade_count += 1
                                        self.active_trades[best_coin] = {
                                            "start_time": time.time(),
                                            "funding_cycle_end": next_funding_time / 1000 if next_funding_time else time.time() + 8 * 3600,
                                            "side_paradex": side_paradex,
                                            "side_hyperliquid": side_hyperliquid,
                                            "fr_paradex": funding_diffs[best_coin]["fr_paradex"],
                                            "fr_hyperliquid": funding_diffs[best_coin]["fr_hyperliquid"],
                                            "predicted_hl_rate": predicted_hl_rate,
                                            "paradex": {"side": side_paradex, "size": size, "entry_price": paradex_limit_price, "order_id": paradex_order_id, "pnl": 0.0},
                                            "hyperliquid": {"side": side_hyperliquid, "size": size, "entry_price": hyperliquid_limit_price, "order_id": hyperliquid_order_id, "pnl": 0.0},
                                            "oi_paradex": oi_paradex,
                                            "oi_hyperliquid": oi_hyperliquid
                                        }
                                        daily_pnl = 3 * abs(best_diff) * self.notional
                                        total_cost = (self.paradex_fees["maker"] + self.hyperliquid_fees["maker"]) * self.notional
                                        trade_pnl_net = daily_pnl - total_cost
                                        self.update_queue.put(("update_ui", None, {
                                            "coin": best_coin,
                                            "fr_paradex": fr_paradex,
                                            "fr_hyperliquid": fr_hyperliquid,
                                            "predicted_hl_rate": predicted_hl_rate,
                                            "pnl_net": trade_pnl_net,
                                            "oi_paradex": oi_paradex,
                                            "oi_hyperliquid": oi_hyperliquid
                                        }))
                                        logger.info(f"Trade initiated for {best_coin}: Paradex {side_paradex} at ${paradex_limit_price}, Hyperliquid {side_hyperliquid} at ${hyperliquid_limit_price}")
                                    else:
                                        logger.error(f"Trade failed for {best_coin}")
                                        if paradex_order_id:
                                            self.paradex.cancel_order(paradex_order_id)
                                        if hyperliquid_order_id:
                                            self.hyperliquid.cancel_order(best_coin, hyperliquid_order_id)

                logger.debug("Completed one iteration of bot loop")
                self.update_positions_real_time()
                time.sleep(30)
            except Exception as e:
                logger.error(f"Error in bot loop: {e}", exc_info=True)
                time.sleep(30)

    def process_queue(self):
        """Process updates from the queue and schedule the next check."""
        try:
            while not self.update_queue.empty():
                action, funding_diffs, best_trade = self.update_queue.get()
                if action == "update_ui":
                    self.update_ui(funding_diffs, best_trade)
                self.update_queue.task_done()
            self.root.after(100, self.process_queue)  # Ensure continuous scheduling
        except Exception as e:
            logger.error(f"Error processing queue: {e}", exc_info=True)
            self.root.after(100, self.process_queue)  # Continue even on error

    def sort_column(self, col, reverse):
        """Sort the main Treeview by column."""
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=reverse, key=lambda t: float(t[0].replace('%', '')) if col in ["Diff", "Paradex Rate", "Hyperliquid Rate", "Predicted HL Rate", "Risk", "Liquidity", "OI Paradex", "OI Hyperliquid"] else float(t[0].replace('$', '').replace(',', '')) if col == "PNL" else t[0])
        for index, (val, k) in enumerate(l):
            self.tree.move(k, '', index)
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))

    def sort_column_positions(self, col, reverse):
        """Sort the positions Treeview by column."""
        l = [(self.positions_tree.set(k, col), k) for k in self.positions_tree.get_children('')]
        l.sort(reverse=reverse, key=lambda t: float(t[0].replace('%', '').replace('$', '').replace('hrs', '')) if col in ["FR_Paradex", "FR_Hyperliquid", "Predicted HL Rate", "PNL_RealTime", "Fees_Paradex", "Fees_Hyperliquid", "OI Paradex", "OI Hyperliquid"] else float(t[0].split(':')[0]) * 3600 + float(t[0].split(':')[1]) * 60 + float(t[0].split(':')[2]) if col == "Time_Remaining" else t[0])
        for index, (val, k) in enumerate(l):
            self.positions_tree.move(k, '', index)
        self.positions_tree.heading(col, command=lambda: self.sort_column_positions(col, not reverse))

    def update_ui(self, funding_diffs=None, best_trade=None):
        """Update the UI with market data and trade details."""
        try:
            if funding_diffs is not None:
                for item in self.tree.get_children():
                    self.tree.delete(item)
                if not funding_diffs:
                    self.tree.insert("", "end", values=("No markets available", "-", "-", "-", "-", "-", "-", "-", "-", "-"))
                    return
                for coin, info in funding_diffs.items():
                    diff = info["diff"]
                    fr_paradex = info.get("fr_paradex", 0)
                    fr_hyperliquid = info.get("fr_hyperliquid", 0)
                    predicted_hl_rate, _ = self.hyperliquid.fetch_predicted_funding(coin) or (0, 0)
                    oi_paradex = info.get("oi_paradex", 0)
                    oi_hyperliquid = info.get("oi_hyperliquid", 0)
                    risk = self.calculate_risk(coin, diff)
                    liquidity = self.check_liquidity_paradex_hyperliquid(coin, self.trade_size)
                    daily_pnl = 3 * abs(diff) * self.notional
                    total_cost = (self.paradex_fees["maker"] + self.hyperliquid_fees["maker"]) * self.notional
                    trade_pnl_net = daily_pnl - total_cost if abs(diff) >= self.min_diff_threshold else 0
                    diff_percent = diff * 100
                    color_tag = 'green_text' if abs(diff_percent) >= self.min_diff_threshold * 100 else 'gray_text'
                    self.tree.insert("", "end", values=(
                        coin,
                        f"{fr_paradex*100:.4f}%" if fr_paradex else "-",
                        f"{fr_hyperliquid*100:.4f}%" if fr_hyperliquid else "-",
                        f"{predicted_hl_rate*100:.4f}%" if predicted_hl_rate else "-",
                        f"{diff_percent:.4f}%",
                        f"${trade_pnl_net:,.2f}",
                        f"{risk:.2f}%",
                        f"${liquidity:.2f}",
                        f"{oi_paradex:.2f}",
                        f"{oi_hyperliquid:.2f}"
                    ), tags=(color_tag,) if abs(diff_percent) >= self.min_diff_threshold * 100 else ())

            if best_trade:
                coin = best_trade["coin"]
                fr_paradex = best_trade["fr_paradex"]
                fr_hyperliquid = best_trade["fr_hyperliquid"]
                predicted_hl_rate = best_trade.get("predicted_hl_rate", 0)
                trade_pnl_net = best_trade["pnl_net"]
                oi_paradex = best_trade.get("oi_paradex", 0)
                oi_hyperliquid = best_trade.get("oi_hyperliquid", 0)
                total_fees = (self.paradex_fees["maker"] + self.hyperliquid_fees["maker"]) * self.notional
                self.trade_log.insert(tk.END, f"[TRADE] {coin} - PNL Net: ${trade_pnl_net:.2f}, Total Fees: ${total_fees:.2f}\n")
                self.trade_log.insert(tk.END, f"[TRADE UPDATE] Balances - Paradex: ${self.paradex.balances.get('USDC', 0.0):.2f}, Hyperliquid: ${self.hyperliquid.balances.get('USDC', 0.0):.2f}\n")
                logger.info(f"Trade completed for {coin} with PNL: ${trade_pnl_net:.2f}")
                self.check_alerts(coin, trade_pnl_net)

            self.paradex_balance_label.config(text=f"Paradex: ${self.paradex.balances.get('USDC', 0.0):.2f}")
            self.hyperliquid_balance_label.config(text=f"Hyperliquid: ${self.hyperliquid.balances.get('USDC', 0.0):.2f}")
            total_pnl_balances = (self.paradex.balances.get("USDC", 0) - 10000) + (self.hyperliquid.balances.get("USDC", 0) - 10000)
            self.total_pnl_label.config(text=f"Total PNL (Balances): ${total_pnl_balances:.2f}")
            if time.time() - self.last_balance_print >= 3600:
                self.trade_log.insert(tk.END, f"[HOURLY UPDATE] Balances - Paradex: ${self.paradex.balances.get('USDC', 0.0):.2f}, Hyperliquid: ${self.hyperliquid.balances.get('USDC', 0.0):.2f}\n")
                self.last_balance_print = time.time()
            self.tree.update()
            self.update_positions_real_time()
            self.root.after(1000, self.update_ui)
        except Exception as e:
            logger.error(f"Error updating UI: {e}", exc_info=True)

    def check_liquidity_paradex_hyperliquid(self, coin, order_size):
        """Check combined liquidity from both exchanges."""
        try:
            paradex_liquidity = self.paradex.check_liquidity(coin, order_size) or 0
            hyperliquid_liquidity = self.hyperliquid.check_liquidity(coin, order_size) or 0
            total_liquidity = (paradex_liquidity if paradex_liquidity >= order_size * 1.5 else 0) + (hyperliquid_liquidity if hyperliquid_liquidity >= order_size * 1.5 else 0)
            return total_liquidity
        except Exception as e:
            logger.error(f"Error checking liquidity for {coin}: {e}")
            return 0

    def calculate_position_pnl(self):
        """Calculate the total profit/loss for active positions."""
        total_pnl = 0.0
        for coin, trade in self.active_trades.items():
            paradex_pos = trade.get("paradex", {})
            hyperliquid_pos = trade.get("hyperliquid", {})
            current_paradex_price = self.paradex.fetch_current_price(coin)
            current_hyperliquid_price = self.hyperliquid.fetch_current_price(coin)
            if current_paradex_price is None or current_hyperliquid_price is None:
                continue
            if paradex_pos:
                entry_price = paradex_pos["entry_price"]
                size = paradex_pos["size"]
                side = paradex_pos["side"].lower()
                pnl = (current_paradex_price - entry_price) * size if side == "buy" else (entry_price - current_paradex_price) * size
                paradex_pos["pnl"] = pnl - (self.paradex_fees["maker"] * self.notional)
                total_pnl += paradex_pos["pnl"]
            if hyperliquid_pos:
                entry_price = hyperliquid_pos["entry_price"]
                size = hyperliquid_pos["size"]
                side = hyperliquid_pos["side"].lower()
                pnl = (current_hyperliquid_price - entry_price) * size if side == "buy" else (entry_price - current_hyperliquid_price) * size
                hyperliquid_pos["pnl"] = pnl - (self.hyperliquid_fees["maker"] * self.notional)
                total_pnl += hyperliquid_pos["pnl"]
            if paradex_pos and hyperliquid_pos:
                net_pnl = abs(paradex_pos["pnl"]) - abs(hyperliquid_pos["pnl"]) if paradex_pos["pnl"] * hyperliquid_pos["pnl"] < 0 else 0
                total_pnl = net_pnl
        return total_pnl

    def update_positions_real_time(self):
        """Update the positions Treeview with real-time data."""
        try:
            for item in self.positions_tree.get_children():
                self.positions_tree.delete(item)
            for coin, trade in self.active_trades.items():
                time_remaining_seconds = trade.get("funding_cycle_end", time.time() + 3 * 24 * 3600) - time.time()
                if time_remaining_seconds <= 0:
                    self.close_position(coin)
                    continue
                hours, rem = divmod(int(time_remaining_seconds), 3600)
                minutes, seconds = divmod(rem, 60)
                time_remaining = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                fr_paradex = self.paradex.fetch_current_funding(coin) or trade.get("fr_paradex", 0)
                fr_hyperliquid = self.hyperliquid.get_current_funding(coin) or trade.get("fr_hyperliquid", 0)
                predicted_hl_rate, _ = self.hyperliquid.fetch_predicted_funding(coin) or (0, 0)
                oi_paradex = trade.get("oi_paradex", 0)
                oi_hyperliquid = trade.get("oi_hyperliquid", 0)
                self.calculate_position_pnl()
                paradex_pos = trade.get("paradex", {})
                hyperliquid_pos = trade.get("hyperliquid", {})
                pnl_real_time = (paradex_pos.get("pnl", 0.0) + hyperliquid_pos.get("pnl", 0.0)) if not (paradex_pos and hyperliquid_pos) else abs(paradex_pos.get("pnl", 0.0)) - abs(hyperliquid_pos.get("pnl", 0.0))
                order_status = "Open"
                if time.time() - trade.get("start_time", 0) > 3 * 24 * 3600 or pnl_real_time > 10:
                    self.close_position(coin)
                    order_status = "Closed"
                action_button = ttk.Button(self.positions_tree, text="Cancel", command=lambda c=coin: self.cancel_trade_manually(c))
                self.positions_tree.insert("", "end", values=(
                    coin,
                    paradex_pos.get("side", "-"),
                    hyperliquid_pos.get("side", "-"),
                    f"{fr_paradex*100:.4f}%" if fr_paradex else "-",
                    f"{fr_hyperliquid*100:.4f}%" if fr_hyperliquid else "-",
                    f"{predicted_hl_rate*100:.4f}%" if predicted_hl_rate else "-",
                    f"${pnl_real_time:.2f}",
                    f"${self.paradex_fees['maker'] * self.notional:.2f}" if paradex_pos else "-",
                    f"${self.hyperliquid_fees['maker'] * self.notional:.2f}" if hyperliquid_pos else "-",
                    time_remaining,
                    order_status,
                    f"{oi_paradex:.2f}",
                    f"{oi_hyperliquid:.2f}",
                    ""
                ))
                self.positions_tree.set(self.positions_tree.get_children()[-1], "Action", action_button)
        except Exception as e:
            logger.error(f"Error updating positions real-time: {e}", exc_info=True)

    def close_position(self, coin):
        """Close an active position by placing opposing orders."""
        try:
            if coin not in self.active_trades:
                logger.warning(f"No active trade for {coin} to close")
                return
            trade = self.active_trades[coin]
            paradex_price = self.paradex.fetch_current_price(coin)
            hyperliquid_price = self.hyperliquid.fetch_current_price(coin)
            if paradex_price is None or hyperliquid_price is None:
                logger.error(f"Cannot fetch current prices to close position for {coin}")
                return
            price_buffer = 0.001
            paradex_side = "buy" if trade.get("side_paradex", "long") == "short" else "sell"
            hyperliquid_side = "sell" if trade.get("side_hyperliquid", "long") == "short" else "buy"
            paradex_close_price = paradex_price * (1 - price_buffer if paradex_side == "sell" else 1 + price_buffer)
            hyperliquid_close_price = hyperliquid_price * (1 - price_buffer if hyperliquid_side == "sell" else 1 + price_buffer)
            size = trade.get("paradex", {}).get("size", 0) or trade.get("hyperliquid", {}).get("size", 0)
            paradex_order_id = self.paradex.place_limit_order(coin, paradex_side, paradex_close_price, size, reduce_only=True) if trade.get("paradex") else None
            hyperliquid_order_id = self.hyperliquid.place_limit_order(coin, hyperliquid_side, hyperliquid_close_price, size, reduce_only=True) if trade.get("hyperliquid") else None
            if paradex_order_id or hyperliquid_order_id:
                logger.info(f"Closing position for {coin}: Paradex {paradex_side} at ${paradex_close_price}, Hyperliquid {hyperliquid_side} at ${hyperliquid_close_price}")
                if paradex_order_id:
                    trade["paradex_close_order_id"] = paradex_order_id
                if hyperliquid_order_id:
                    trade["hyperliquid_close_order_id"] = hyperliquid_order_id
                trade["close_start_time"] = time.time()
                del self.active_trades[coin]
                self.update_positions_real_time()
            else:
                logger.error(f"Failed to place closing orders for {coin}")
                if paradex_order_id:
                    self.paradex.cancel_order(paradex_order_id)
                if hyperliquid_order_id:
                    self.hyperliquid.cancel_order(coin, hyperliquid_order_id)
        except Exception as e:
            logger.error(f"Error closing position for {coin}: {e}", exc_info=True)

    def cancel_trade_manually(self, coin):
        """Manually cancel an active trade."""
        try:
            if coin in self.active_trades:
                trade = self.active_trades[coin]
                for order_type in ["paradex", "hyperliquid"]:
                    order_data = trade.get(order_type, {})
                    if order_data and "order_id" in order_data:
                        client = getattr(self, f"{order_type}")
                        if order_type == "paradex":
                            client.cancel_order(order_data["order_id"])
                        else:
                            client.cancel_order(coin, order_data["order_id"])
                    if trade.get(f"{order_type}_close_order_id"):
                        if order_type == "paradex":
                            client.cancel_order(trade[f"{order_type}_close_order_id"])
                        else:
                            client.cancel_order(coin, trade[f"{order_type}_close_order_id"])
                del self.active_trades[coin]
                logger.info(f"Manually cancelled trade for {coin}")
                self.update_positions_real_time()
        except Exception as e:
            logger.error(f"Error cancelling trade manually for {coin}: {e}", exc_info=True)

    def check_alerts(self, coin, pnl):
        """Check and display trade alerts."""
        try:
            if pnl > 50:
                messagebox.showinfo("Alert PNL", f"High PNL for {coin}: ${pnl:.2f}")
            elif pnl < self.max_loss_threshold * 0.5:
                messagebox.showwarning("Alert Loss", f"Critical loss for {coin}: ${pnl:.2f}")
        except Exception as e:
            logger.error(f"Error checking alerts for {coin}: {e}")

    def show_alerts(self):
        """Display overall risk alerts."""
        try:
            total_pnl = (self.paradex.balances.get("USDC", 0) - 10000) + (self.hyperliquid.balances.get("USDC", 0) - 10000)
            if total_pnl < self.max_loss_threshold * 0.5:
                messagebox.showwarning("Risk Alert", f"Approaching maximum loss threshold: Total PNL = ${total_pnl:.2f}")
        except Exception as e:
            logger.error(f"Error showing alerts: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    try:
        paradex_client = ParadexClient()
    except Exception as e:
        logger.error(f"Failed to initialize ParadexClient: {e}")
        messagebox.showerror("Initialization Error", f"Failed to initialize ParadexClient: {e}")
        sys.exit(1)

    try:
        hyperliquid_client = HyperliquidClient()
    except Exception as e:
        logger.error(f"Failed to initialize HyperliquidClient: {e}")
        messagebox.showerror("Initialization Error", f"Failed to initialize HyperliquidClient: {e}")
        sys.exit(1)

    # Initialize the bot with specified parameters
    bot = LiveArbitrageBot(
        paradex_client,
        hyperliquid_client,
        trade_size=10000,
        leverage=1,
        min_diff_threshold=0.001,
        max_trades_per_day=10,
        max_loss_threshold=500,
        root=root
    )
    bot.start_real_time_update()
    root.mainloop()