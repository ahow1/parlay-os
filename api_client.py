"""PARLAY OS — api_client.py
Unified HTTP client for all outbound GET requests.

Features:
  • 5-minute response cache per URL (memory backend)
  • 1 req/sec rate limit per domain
  • Exponential backoff: 1s → 2s → 4s → 8s, 4 attempts total
  • Circuit breaker: 3 consecutive failures → 10-minute pause per domain
  • skip_cache=True for real-time endpoints (health checks, live monitor)

Usage:
    from api_client import get as _http_get
    r = _http_get("https://...", timeout=10)
"""

import time
import threading
import logging
from urllib.parse import urlparse

import requests
import requests_cache

log = logging.getLogger(__name__)

# ── Cache setup ───────────────────────────────────────────────────────────────

_session = requests_cache.CachedSession(
    cache_name="parlay_http_cache",
    backend="memory",
    expire_after=300,          # 5-minute TTL
    allowable_codes=[200],
    allowable_methods=["GET"],
    stale_if_error=True,       # serve stale on error
)
_session.headers.update({"User-Agent": "ParlayOS/2.0"})


# ── Rate limiter ──────────────────────────────────────────────────────────────

_rate_lock = threading.Lock()
_last_call: dict[str, float] = {}
_MIN_INTERVAL = 1.0  # seconds between requests to same domain


def _domain(url: str) -> str:
    return urlparse(url).netloc


def _rate_limit(domain: str):
    with _rate_lock:
        now  = time.monotonic()
        wait = _MIN_INTERVAL - (now - _last_call.get(domain, 0.0))
        if wait > 0:
            time.sleep(wait)
        _last_call[domain] = time.monotonic()


# ── Circuit breaker ───────────────────────────────────────────────────────────

_circuit_lock = threading.Lock()
_circuit: dict[str, dict] = {}   # domain → {failures, tripped_at}
_CIRCUIT_THRESHOLD = 3
_CIRCUIT_TIMEOUT   = 600  # 10 minutes


def _circuit_open(domain: str) -> bool:
    with _circuit_lock:
        state = _circuit.get(domain)
        if not state:
            return False
        tripped = state.get("tripped_at")
        if tripped and time.monotonic() - tripped > _CIRCUIT_TIMEOUT:
            del _circuit[domain]
            log.info(f"[circuit] {domain} reset after 10-min cooldown")
            return False
        return bool(tripped)


def _circuit_success(domain: str):
    with _circuit_lock:
        _circuit.pop(domain, None)


def _circuit_failure(domain: str):
    with _circuit_lock:
        state = _circuit.setdefault(domain, {"failures": 0})
        state["failures"] += 1
        if state["failures"] >= _CIRCUIT_THRESHOLD and "tripped_at" not in state:
            state["tripped_at"] = time.monotonic()
            log.error(
                f"[circuit] {domain} TRIPPED after {state['failures']} consecutive "
                f"failures — pausing {_CIRCUIT_TIMEOUT // 60} min"
            )


# ── Main entry point ──────────────────────────────────────────────────────────

def get(
    url: str,
    *,
    skip_cache: bool = False,
    **kwargs,
) -> requests.Response:
    """
    Drop-in replacement for requests.get().
    Raises requests.exceptions.RequestException after all retries exhausted.
    Pass skip_cache=True for real-time endpoints that must never return stale data.
    """
    domain = _domain(url)

    if _circuit_open(domain):
        raise requests.exceptions.ConnectionError(
            f"[circuit] {domain} is tripped — skipping call"
        )

    _rate_limit(domain)

    delays  = [1, 2, 4, 8]
    last_ex: Exception | None = None

    for attempt, delay in enumerate(delays):
        try:
            if skip_cache:
                r = requests.get(url, **kwargs)
            else:
                r = _session.get(url, **kwargs)

            r.raise_for_status()
            _circuit_success(domain)
            return r

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status == 429:
                log.warning(f"[api_client] 429 rate limit on {domain}, retrying in {delay}s")
            elif 400 <= status < 500:
                # Client error — no point retrying
                _circuit_failure(domain)
                raise
            else:
                log.warning(f"[api_client] {url} attempt {attempt + 1}/4 HTTP {status}")
            last_ex = exc

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as exc:
            log.warning(f"[api_client] {url} attempt {attempt + 1}/4 network error: {exc}")
            last_ex = exc

        except Exception as exc:
            log.warning(f"[api_client] {url} attempt {attempt + 1}/4 unexpected: {exc}")
            last_ex = exc

        if attempt < len(delays) - 1:
            time.sleep(delay)

    _circuit_failure(domain)
    raise requests.exceptions.RequestException(
        f"All 4 attempts failed for {url}: {last_ex}"
    ) from last_ex


def circuit_status() -> dict:
    """Return current circuit breaker state for all domains."""
    with _circuit_lock:
        result = {}
        for domain, state in _circuit.items():
            tripped = state.get("tripped_at")
            result[domain] = {
                "failures": state.get("failures", 0),
                "tripped":  bool(tripped),
                "resets_in": (
                    round(_CIRCUIT_TIMEOUT - (time.monotonic() - tripped))
                    if tripped else None
                ),
            }
        return result
