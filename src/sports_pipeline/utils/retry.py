"""Retry utilities using tenacity."""

from __future__ import annotations

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Standard retry for HTTP requests
http_retry = retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)

# Retry for rate-limited requests (429 status)
rate_limit_retry = retry(
    retry=retry_if_exception_type(requests.HTTPError),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    stop=stop_after_attempt(3),
    reraise=True,
)
