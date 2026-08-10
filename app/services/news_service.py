"""
News data fetching service — RapidAPI Real-Time News Data.
"""

import logging

import requests

from app.core.config import settings

logger = logging.getLogger(__name__)


def fetch_news(
    query: str,
    limit: int = 5,
    time_published: str = "anytime",
    country: str = "US",
    lang: str = "en",
) -> dict | None:
    """
    Fetch news articles from RapidAPI Real-Time News Data API.

    Args:
        query: Search topic (e.g. "AI", "Tesla", "Football").
        limit: Number of articles to return.
        time_published: Time filter — anytime | 1h | 1d | 7d | 1y.
        country: ISO country code.
        lang: Language code.

    Returns:
        Parsed JSON response dict, or None on failure.

    Raises:
        ValueError: If the API key is missing.
        requests.RequestException: On network / HTTP errors.
    """
    api_key = settings.RAPID_API_KEY
    if not api_key:
        raise ValueError("RAPID_API_KEY is not configured in the environment.")

    url = "https://real-time-news-data.p.rapidapi.com/search"

    headers = {
        "x-rapidapi-host": "real-time-news-data.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }

    params = {
        "query": query,
        "limit": str(limit),
        "time_published": time_published,
        "country": country,
        "lang": lang,
    }

    logger.info("Fetching news for '%s' (limit=%d, time=%s)", query, limit, time_published)
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    return response.json()
