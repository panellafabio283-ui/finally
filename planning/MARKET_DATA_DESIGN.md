# Market Data Backend — Design & Implementation Guide

Complete implementation reference for the FinAlly market data subsystem. All code lives under `backend/app/market/`.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Data Model — `models.py`](#3-data-model)
4. [Price Cache — `cache.py`](#4-price-cache)
5. [Abstract Interface — `interface.py`](#5-abstract-interface)
6. [Seed Prices & Parameters — `seed_prices.py`](#6-seed-prices--parameters)
7. [GBM Simulator — `simulator.py`](#7-gbm-simulator)
8. [Massive API Client — `massive_client.py`](#8-massive-api-client)
9. [Factory — `factory.py`](#9-factory)
10. [SSE Streaming Endpoint — `stream.py`](#10-sse-streaming-endpoint)
11. [FastAPI Lifecycle Integration](#11-fastapi-lifecycle-integration)
12. [Watchlist Coordination](#12-watchlist-coordination)
13. [Testing Strategy](#13-testing-strategy)
14. [Error Handling & Edge Cases](#14-error-handling--edge-cases)
15. [Configuration Reference](#15-configuration-reference)

---

## 1. Architecture Overview

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼ writes to
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

The **strategy pattern** keeps downstream code source-agnostic. Both implementations push price updates into a shared `PriceCache`. Consumers (SSE, trades, portfolio) read only from the cache — they never call the data source directly.

### Data flow

```
[Simulator loop every 500ms]     [Massive poller every 15s]
         │                                  │
         └──────────────┬───────────────────┘
                        ▼
                   PriceCache.update(ticker, price)
                        │
                        ├──→ SSE generator reads cache every 500ms → client EventSource
                        ├──→ POST /api/portfolio/trade reads cache.get_price(ticker)
                        └──→ GET /api/portfolio reads cache.get_all()
```

---

## 2. File Structure

```
backend/
  app/
    market/
      __init__.py         # Public re-exports
      models.py           # PriceUpdate dataclass
      cache.py            # PriceCache (thread-safe in-memory store)
      interface.py        # MarketDataSource ABC
      seed_prices.py      # SEED_PRICES, TICKER_PARAMS, CORRELATION_GROUPS
      simulator.py        # GBMSimulator + SimulatorDataSource
      massive_client.py   # MassiveDataSource (Polygon.io REST poller)
      factory.py          # create_market_data_source() — selects source
      stream.py           # FastAPI SSE endpoint router
```

**`__init__.py`** re-exports the public surface:

```python
from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream code imports from `app.market`, not from submodules:

```python
from app.market import PriceCache, create_market_data_source, create_stream_router
```

---

## 3. Data Model

**File: `backend/app/market/models.py`**

`PriceUpdate` is the only type that crosses the market data layer boundary. Every consumer — SSE, portfolio, trade execution — works exclusively with this type.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`**: Price updates are immutable value objects. Safe to share across async tasks without copying or locking.
- **`slots=True`**: Minor memory optimization — thousands of these are created per session.
- **Computed properties** (`change`, `direction`, `change_percent`): Derived on demand so they can never be stale or inconsistent with the stored prices.
- **`to_dict()`**: Single serialization point used by both SSE and REST API responses — no duplicate serialization logic.

---

## 4. Price Cache

**File: `backend/app/market/cache.py`**

Central data hub. Data sources write to it; SSE streaming and portfolio valuation read from it. Thread-safe because the Massive poller runs in `asyncio.to_thread()` (a real OS thread) while SSE reads happen on the async event loop.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Bumped on every write; used by SSE for change detection

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price. Returns the created PriceUpdate.

        On first update for a ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        """Get the latest PriceUpdate for a ticker, or None if unknown."""
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: get just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        """Remove a ticker from the cache (e.g., when removed from watchlist)."""
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonically increasing counter. Bumped on every update."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Version counter — why it exists

The SSE loop polls the cache every 500ms. Without a version counter, it would serialize and transmit all prices every tick even if nothing changed (Massive API only updates every 15s). The version counter lets SSE skip no-op sends:

```python
last_version = -1
while True:
    if price_cache.version != last_version:
        last_version = price_cache.version
        yield format_sse(price_cache.get_all())
    await asyncio.sleep(0.5)
```

### Why `threading.Lock` not `asyncio.Lock`

`asyncio.Lock` only protects code running on the event loop. The Massive client's `_fetch_snapshots()` runs via `asyncio.to_thread()` — a real OS thread. `threading.Lock` correctly serializes access from both sync threads and the async event loop.

---

## 5. Abstract Interface

**File: `backend/app/market/interface.py`**

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Both implementations push price updates into the shared PriceCache on their
    own schedule. Downstream code reads from the cache; it never calls the
    data source for prices.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")       # watchlist grows
        await source.remove_ticker("GOOGL")   # watchlist shrinks
        await source.stop()                   # app shutdown
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.

        Starts a background task that writes to the PriceCache.
        Must be called exactly once. Calling start() twice is undefined.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources.

        Safe to call multiple times. After stop(), no further cache writes occur.
        """

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set and from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

### Push vs pull

The source writes to the cache (push model) rather than returning prices on demand (pull model). This decouples producer timing from consumer timing: the simulator ticks at 500ms, Massive polls at 15s, but SSE always reads from cache at its own 500ms cadence. No consumer needs to know which source is active.

---

## 6. Seed Prices & Parameters

**File: `backend/app/market/seed_prices.py`**

Constants only — no logic, no imports. Shared by the simulator (initial prices and GBM parameters) and available as a fallback for the Massive client if the API hasn't responded yet.

```python
"""Seed prices and per-ticker GBM parameters for the market simulator."""

# Realistic starting prices for the 10 default watchlist tickers
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more movement per tick)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # High volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # High volatility, strong upward drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # Low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for dynamically added tickers not in TICKER_PARAMS
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Sector groupings for the Cholesky correlation matrix
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech":    {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Pairwise correlation coefficients
INTRA_TECH_CORR    = 0.6   # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR   = 0.3   # Between-sector baseline
TSLA_CORR          = 0.3   # TSLA does its own thing
```

---

## 7. GBM Simulator

**File: `backend/app/market/simulator.py`**

Two classes with a clean separation of concerns:

- **`GBMSimulator`**: Pure math engine. Stateful — holds current prices and advances them one step at a time. Has no knowledge of asyncio or FastAPI.
- **`SimulatorDataSource`**: The `MarketDataSource` implementation that wraps `GBMSimulator` in an async background loop, writing to `PriceCache`.

### 7.1 GBM Math

Geometric Brownian Motion is the standard model underlying Black-Scholes option pricing. At each time step:

```
S(t+dt) = S(t) × exp( (μ - σ²/2) × dt  +  σ × √dt × Z )
```

Where:
- `S(t)` — current price
- `μ` (mu) — annualized drift (expected return), e.g. `0.05` (5%)
- `σ` (sigma) — annualized volatility, e.g. `0.22` (22%)
- `dt` — time step as a fraction of a trading year
- `Z` — standard normal random variable

For 500ms ticks with 252 trading days and 6.5 hours/day:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # = 5,896,800
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ≈ 8.48e-8
```

This tiny `dt` produces sub-cent moves per tick that accumulate naturally over simulated time. Prices can never go negative because `exp()` is always positive.

### 7.2 Correlated Moves with Cholesky Decomposition

Real stocks don't move independently — tech stocks tend to move together. We use a **Cholesky decomposition** of a sector correlation matrix to generate correlated random draws.

Given a positive-definite correlation matrix `C`, compute lower triangular `L = cholesky(C)`. Then for `n` independent standard normals `Z_independent ∈ ℝⁿ`:

```
Z_correlated = L @ Z_independent
```

The resulting `Z_correlated` has covariance matrix `C` by construction. Each ticker uses its entry from `Z_correlated` as the stochastic term in the GBM formula.

### 7.3 GBMSimulator Implementation

```python
from __future__ import annotations

import math
import random
import logging

import numpy as np

from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        # Batch-initialize without rebuilding Cholesky on each add
        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.

        This is the hot path — called every 500ms.
        """
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma ** 2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1% chance per tick — adds visual drama
            # With 10 tickers at 2 ticks/sec, expect ~1 event every 50 seconds
            if random.random() < self._event_prob:
                magnitude = random.uniform(0.02, 0.05)
                sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + magnitude * sign
                logger.debug(
                    "Random event %s: %.1f%% %s",
                    ticker, magnitude * 100, "up" if sign > 0 else "down",
                )

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker. Rebuilds Cholesky matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker. Rebuilds Cholesky matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """Rebuild the Cholesky decomposition of the correlation matrix.

        Called on every add/remove. O(n²) but n stays small (<50 tickers).
        """
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Sector-based pairwise correlation.

        Tech–tech: 0.6, Finance–finance: 0.5, TSLA with anything: 0.3, else: 0.3
        """
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

### 7.4 SimulatorDataSource — Async Wrapper

```python
import asyncio
from .cache import PriceCache
from .interface import MarketDataSource


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by GBMSimulator.

    Runs a background asyncio task calling GBMSimulator.step() every
    `update_interval` seconds and writing results to the PriceCache.
    """

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)

        # Seed the cache immediately so SSE has data on its very first tick
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)

        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        """Core loop: step simulation, write to cache, sleep."""
        while True:
            try:
                if self._sim:
                    prices = self._sim.step()
                    for ticker, price in prices.items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Key behaviors:**
- **Immediate seeding**: `start()` populates the cache with seed prices *before* the async loop begins, so SSE has prices to send on its very first tick.
- **Graceful cancellation**: `stop()` cancels the task and awaits it to catch `CancelledError`. Required for clean FastAPI lifespan shutdown.
- **Exception resilience**: The loop catches per-step exceptions so a single bad tick doesn't kill the entire feed.

---

## 8. Massive API Client

**File: `backend/app/market/massive_client.py`**

Polls the Massive (formerly Polygon.io) REST API snapshot endpoint. The synchronous Massive client runs via `asyncio.to_thread()` to avoid blocking the event loop.

### 8.1 Massive API Reference

**Package**: `massive` (`uv add massive`)  
**Base URL**: `https://api.massive.com` (legacy `https://api.polygon.io` still works)  
**Auth**: API key via `MASSIVE_API_KEY` env var or `RESTClient(api_key=...)`

**Primary endpoint used** — All-ticker snapshot (one API call for all tickers):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="your_key")
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    # snap.last_trade.price     → current price (float)
    # snap.last_trade.timestamp → Unix milliseconds (int)
    # snap.day.previous_close   → prior day close (float)
    # snap.day.change_percent   → day change % (float)
```

**Rate limits:**
- Free tier: 5 requests/minute → poll every 15s
- Paid tiers: effectively unlimited → poll every 2–5s

### 8.2 MassiveDataSource Implementation

```python
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call per interval.
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: Any = None

    async def start(self, tickers: list[str]) -> None:
        from massive import RESTClient
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        # Immediate first poll so the cache has data before the loop starts
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval", len(tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """One poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return
        try:
            # Massive RESTClient is synchronous — run in thread to avoid blocking
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)

            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))

        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise — retry on next interval

    def _fetch_snapshots(self) -> list:
        """Synchronous Massive API call. Must run in a thread."""
        from massive.rest.models import SnapshotMarketType
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### 8.3 Error Handling

| Error | Behavior |
|-------|----------|
| **401 Unauthorized** | Logged as error. Poller continues (restart after fixing `.env`). |
| **429 Rate Limited** | Logged as error. Retries after `poll_interval` seconds. |
| **Network timeout** | Logged as error. Retries automatically on next cycle. |
| **Malformed snapshot** | Individual ticker skipped with warning; others still processed. |
| **All tickers fail** | Cache retains last-known prices; SSE streams stale data (better than nothing). |

### 8.4 Note on the `massive` import

`from massive import RESTClient` is deferred to `start()`, not module-level. This means:
- The `massive` package is only needed when `MASSIVE_API_KEY` is set.
- Users without an API key can run with the simulator without installing `massive`.
- The simulator path has zero external dependencies beyond `numpy`.

---

## 9. Factory

**File: `backend/app/market/factory.py`**

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation, default)

    Returns an unstarted source. Caller must: await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        from .massive_client import MassiveDataSource
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        from .simulator import SimulatorDataSource
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

### Usage at app startup

```python
from app.market import PriceCache, create_market_data_source

price_cache = PriceCache()
source = create_market_data_source(price_cache)  # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
                    "NVDA", "META", "JPM", "V", "NFLX"])
```

---

## 10. SSE Streaming Endpoint

**File: `backend/app/market/stream.py`**

The SSE endpoint holds a long-lived HTTP connection and pushes all price updates to connected clients every ~500ms.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory that injects the PriceCache without using globals."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint: GET /api/stream/prices

        Client connects with EventSource. Receives events formatted as:
            data: {"AAPL": {...}, "GOOGL": {...}, ...}

        The 'retry' directive tells the browser to auto-reconnect after
        1 second if the connection drops.
        """
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Async generator yielding SSE-formatted price events.

    Sends all current prices every `interval` seconds. Stops when the
    client disconnects (detected via request.is_disconnected()).
    """
    yield "retry: 1000\n\n"  # Client reconnects after 1s on disconnect

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### SSE wire format

Each event the browser receives:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{"ticker":"GOOGL","price":175.12,...}}

```

(The blank line after `data: ...` is the SSE event delimiter.)

### Frontend consumption

```javascript
const eventSource = new EventSource('/api/stream/prices');

eventSource.onmessage = (event) => {
    const prices = JSON.parse(event.data);
    // prices: { "AAPL": { ticker, price, previous_price, change, change_percent, direction, timestamp }, ... }
    updateWatchlist(prices);
};

eventSource.onerror = () => {
    // EventSource auto-reconnects after the retry directive delay (1s)
    setConnectionStatus('reconnecting');
};
```

### Design: poll-and-push vs event-driven

The SSE generator polls the cache at a fixed interval rather than being notified by the data source. This produces evenly-spaced updates for the frontend — important for sparkline charts that accumulate data points since page load. Regular intervals also simplify the connection status logic (if the browser receives events at the expected cadence, it's connected).

---

## 11. FastAPI Lifecycle Integration

**File: `backend/app/main.py`**

Use FastAPI's `lifespan` context manager to start and stop the market data system with the application.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.interface import MarketDataSource
from app.market.stream import create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---

    # 1. Create the shared price cache (single instance for the entire app)
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    # 2. Create the market data source (reads MASSIVE_API_KEY from env)
    source = create_market_data_source(price_cache)
    app.state.market_source = source

    # 3. Load the initial watchlist from the database
    initial_tickers = await load_watchlist_tickers_from_db()
    await source.start(initial_tickers)

    # 4. Register the SSE router (must happen after startup so cache is ready)
    app.include_router(create_stream_router(price_cache))

    yield  # Application is running

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# --- Dependency functions for route injection ---

def get_price_cache() -> PriceCache:
    return app.state.price_cache

def get_market_source() -> MarketDataSource:
    return app.state.market_source
```

### Injecting into route handlers

```python
from fastapi import APIRouter, Depends, HTTPException
from app.market.cache import PriceCache
from app.market.interface import MarketDataSource

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(
    trade: TradeRequest,
    price_cache: PriceCache = Depends(get_price_cache),
):
    current_price = price_cache.get_price(trade.ticker)
    if current_price is None:
        raise HTTPException(400, f"No price available for {trade.ticker}. Please wait a moment.")
    # ... execute trade at current_price ...


@router.post("/watchlist")
async def add_to_watchlist(
    payload: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    price_cache: PriceCache = Depends(get_price_cache),
):
    await db.insert_watchlist_entry(payload.ticker)
    await source.add_ticker(payload.ticker)
    price = price_cache.get_price(payload.ticker)  # May be None briefly for Massive
    return {"ticker": payload.ticker, "price": price}


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(
    ticker: str,
    source: MarketDataSource = Depends(get_market_source),
):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    # Keep tracking in cache if user still holds shares
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

---

## 12. Watchlist Coordination

When the watchlist changes, the data source must be notified to track the right tickers.

### Adding a ticker

```
User/LLM → POST /api/watchlist {"ticker": "PYPL"}
  1. Insert into watchlist table (SQLite)
  2. await source.add_ticker("PYPL")
       Simulator: adds to GBMSimulator, rebuilds Cholesky, seeds cache immediately
       Massive:   appends to ticker list, appears on next poll (up to 15s delay)
  3. Return {"ticker": "PYPL", "price": price_cache.get_price("PYPL")}
```

### Removing a ticker

```
User/LLM → DELETE /api/watchlist/PYPL
  1. Delete from watchlist table (SQLite)
  2. Check for open position — if held shares, skip remove_ticker() (keep valuation working)
  3. await source.remove_ticker("PYPL")  ← if no open position
       Both sources: remove from active set, remove from PriceCache
  4. Return {"status": "ok"}
```

### Edge case: open position on removed ticker

If the user removes a watchlist ticker but still holds shares, the ticker must remain tracked so portfolio valuation stays accurate. The watchlist route must check for this:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str, ...):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)
    return {"status": "ok"}
```

---

## 13. Testing Strategy

### 13.1 Unit Tests: GBMSimulator

**`backend/tests/market/test_simulator.py`**

```python
import pytest
from app.market.simulator import GBMSimulator
from app.market.seed_prices import SEED_PRICES


class TestGBMSimulator:

    def test_step_returns_all_tickers(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_always_positive(self):
        """GBM exp() is always positive — prices can never go to zero."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_add_ticker(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        assert "TSLA" in sim.step()

    def test_remove_ticker(self):
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_add_duplicate_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim._tickers) == 1

    def test_remove_nonexistent_is_noop(self):
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")  # Should not raise

    def test_unknown_ticker_gets_random_seed(self):
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert 50.0 <= price <= 300.0

    def test_empty_step_returns_empty(self):
        sim = GBMSimulator(tickers=[])
        assert sim.step() == {}

    def test_prices_change_over_time(self):
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(1000):
            sim.step()
        assert sim.get_price("AAPL") != SEED_PRICES["AAPL"]

    def test_cholesky_builds_for_two_plus_tickers(self):
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # No matrix for single ticker
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None
```

### 13.2 Unit Tests: PriceCache

**`backend/tests/market/test_cache.py`**

```python
import pytest
from app.market.cache import PriceCache


class TestPriceCache:

    def test_update_and_get(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove_clears_ticker(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_get_all_returns_all(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        assert set(cache.get_all().keys()) == {"AAPL", "GOOGL"}

    def test_version_increments_on_update(self):
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None
```

### 13.3 Integration Tests: SimulatorDataSource

**`backend/tests/market/test_simulator_source.py`**

```python
import asyncio
import pytest
from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource


@pytest.mark.asyncio
class TestSimulatorDataSource:

    async def test_start_populates_cache_immediately(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL", "GOOGL"])

        # Seed prices are in the cache before the first loop tick
        assert cache.get("AAPL") is not None
        assert cache.get("GOOGL") is not None
        await source.stop()

    async def test_stop_is_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])
        await source.stop()
        await source.stop()  # Double stop should not raise

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.1)
        await source.start(["AAPL"])

        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get("TSLA") is not None

        await source.remove_ticker("TSLA")
        assert "TSLA" not in source.get_tickers()
        assert cache.get("TSLA") is None

        await source.stop()
```

### 13.4 Unit Tests: MassiveDataSource (Mocked)

**`backend/tests/market/test_massive.py`**

```python
from unittest.mock import MagicMock, patch
import pytest
from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource


def _make_snapshot(ticker: str, price: float, timestamp_ms: int) -> MagicMock:
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = timestamp_ms
    return snap


@pytest.mark.asyncio
class TestMassiveDataSource:

    async def test_poll_updates_cache(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "GOOGL"]

        snapshots = [
            _make_snapshot("AAPL", 190.50, 1707580800000),
            _make_snapshot("GOOGL", 175.25, 1707580800000),
        ]
        with patch.object(source, "_fetch_snapshots", return_value=snapshots):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("GOOGL") == 175.25

    async def test_malformed_snapshot_is_skipped(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]

        good = _make_snapshot("AAPL", 190.50, 1707580800000)
        bad = MagicMock()
        bad.ticker = "BAD"
        bad.last_trade = None  # Triggers AttributeError in _poll_once

        with patch.object(source, "_fetch_snapshots", return_value=[good, bad]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            await source._poll_once()  # Must not raise

        assert cache.get_price("AAPL") is None

    async def test_add_and_remove_ticker(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test", price_cache=cache, poll_interval=60.0)
        source._tickers = []

        await source.add_ticker("AAPL")
        assert "AAPL" in source.get_tickers()

        await source.remove_ticker("AAPL")
        assert "AAPL" not in source.get_tickers()
```

### 13.5 Factory Tests

**`backend/tests/market/test_factory.py`**

```python
import pytest
from unittest.mock import patch
from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.simulator import SimulatorDataSource
from app.market.massive_client import MassiveDataSource


def test_no_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict("os.environ", {}, clear=True):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_empty_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict("os.environ", {"MASSIVE_API_KEY": ""}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_whitespace_api_key_returns_simulator():
    cache = PriceCache()
    with patch.dict("os.environ", {"MASSIVE_API_KEY": "   "}):
        source = create_market_data_source(cache)
    assert isinstance(source, SimulatorDataSource)


def test_valid_api_key_returns_massive():
    cache = PriceCache()
    with patch.dict("os.environ", {"MASSIVE_API_KEY": "pk_live_abc123"}):
        source = create_market_data_source(cache)
    assert isinstance(source, MassiveDataSource)
```

---

## 14. Error Handling & Edge Cases

### Empty watchlist on startup

If the database has no watchlist entries, `start([])` is called. Both data sources handle this gracefully — no prices, no API calls. The SSE endpoint sends empty events. When a ticker is added, the source starts tracking it immediately.

### Price cache miss during trade

If a user trades a ticker with no cached price (e.g., just added, Massive hasn't polled yet):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise HTTPException(
        status_code=400,
        detail=f"Price not yet available for {ticker}. Please wait a moment.",
    )
```

The simulator avoids this by seeding the cache in `add_ticker()`. The Massive client has a brief gap — the HTTP 400 with a clear message is the correct user-facing response.

### Invalid Massive API key

If `MASSIVE_API_KEY` is set but invalid, the first poll fails with a 401. The poller logs the error and retries on the next interval. The SSE endpoint streams empty data. The connection status indicator shows "connected" (SSE works, just no data). Fix: correct the key and restart the container.

### Thread safety under load

`PriceCache` uses `threading.Lock` — a mutex. For 10 tickers at 2 updates/second with multiple SSE readers, lock contention is negligible. The critical section (dict lookup + assignment) is microseconds. If contention ever became an issue with hundreds of tickers, a `ReadWriteLock` would solve it, but that is not needed at this scale.

### Simulator numerical stability

- Prices are `round()`ed to 2 decimal places in `GBMSimulator.step()`
- The `exp()` formulation is numerically stable for any finite drift/diffusion
- Prices are always positive (`exp()` maps to `(0, +∞)`)
- Floating-point precision is not a concern for the price magnitudes used

---

## 15. Configuration Reference

| Parameter | Where set | Default | Description |
|-----------|-----------|---------|-------------|
| `MASSIVE_API_KEY` | Environment variable | `""` (empty) | If set and non-empty, uses Massive API; otherwise uses simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5` s | Time between simulator ticks |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0` s | Time between Massive API polls |
| `event_probability` | `GBMSimulator.__init__` | `0.001` | Probability of a random shock per ticker per tick |
| `dt` | `GBMSimulator.__init__` | `~8.5e-8` | GBM time step (fraction of a trading year) |
| SSE push interval | `_generate_events()` | `0.5` s | Time between SSE pushes to each connected client |
| SSE retry directive | `_generate_events()` | `1000` ms | Browser EventSource reconnect delay after disconnect |

### Environment variable behavior

```bash
# Uses GBM simulator (default — no API key needed)
MASSIVE_API_KEY=

# Uses Massive REST API polling every 15 seconds
MASSIVE_API_KEY=pk_live_abc123xyz

# Faster polling for paid Massive tiers
MASSIVE_API_KEY=pk_live_abc123xyz
# (set poll_interval=5.0 in MassiveDataSource constructor)
```
