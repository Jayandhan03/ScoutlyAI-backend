"""
Database service — shared MongoDB access for the scheduler and preferences.

The Next.js app (YourNews) owns user identity, Telegram links and writes the
news preferences. This service reads those same collections so the backend
scheduler can autonomously deliver briefings. Collection names match the
Mongoose model pluralization used by the frontend:
    UserPreference -> "userpreferences"
    TelegramLink   -> "telegramlinks"
"""

import logging

import gridfs
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from app.core.config import settings

logger = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return a lazily-initialised, process-wide Mongo client."""
    global _client
    if _client is None:
        if not settings.MONGODB_URI:
            raise ValueError("MONGODB_URI is not configured — database features unavailable.")
        _client = MongoClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            tz_aware=True,
        )
        logger.info("MongoDB client initialised (db=%s).", settings.MONGODB_DB)
    return _client


def get_db() -> Database:
    return get_client()[settings.MONGODB_DB]


def preferences_collection() -> Collection:
    return get_db()["userpreferences"]


def telegram_links_collection() -> Collection:
    return get_db()["telegramlinks"]


def whatsapp_links_collection() -> Collection:
    return get_db()["whatsapplinks"]


def agents_collection() -> Collection:
    return get_db()["agents"]


def users_collection() -> Collection:
    return get_db()["users"]


def pending_whatsapp_deliveries_collection() -> Collection:
    return get_db()["pendingwhatsappdeliveries"]


def push_subscriptions_collection() -> Collection:
    """FCM registration tokens — written by the Next.js app when a user
    grants notification permission (web button click or native mobile
    prompt), read here at push-send time. See push_service.py."""
    return get_db()["pushsubscriptions"]


def briefings_collection() -> Collection:
    """Permanent per-agent briefing history — audio metadata + transcript,
    one doc per generated briefing (see briefing_service.persist_briefing).
    Mirrored on the frontend by models/Briefing.ts."""
    return get_db()["briefings"]


def notification_logs_collection() -> Collection:
    """In-app notification history — one doc per push notification sent
    (briefing-ready or test), regardless of FCM delivery success. Read/mutated
    (mark-read) by the Next.js app's bell dropdown. Mirrored on the frontend
    by models/NotificationLog.ts. See notification_log_service.py."""
    return get_db()["notificationlogs"]


def get_gridfs() -> gridfs.GridFS:
    """GridFS bucket holding audio that's waiting on a WhatsApp button-tap
    confirmation (see whatsapp_service.queue_whatsapp_delivery)."""
    return gridfs.GridFS(get_db(), collection="whatsappAudio")


def get_briefing_gridfs() -> gridfs.GridFS:
    """GridFS bucket holding permanent briefing audio, read back out by the
    Next.js dashboard for playback. Unlike get_gridfs()'s whatsappAudio
    bucket, nothing here ever expires or gets deleted."""
    return gridfs.GridFS(get_db(), collection="briefingAudio")


def get_chat_id_for_email(email: str) -> str | None:
    """Look up the Telegram chat_id linked to a user's email, if any."""
    doc = telegram_links_collection().find_one({"email": email.lower()})
    return doc.get("chatId") if doc else None


def get_wa_id_for_email(email: str) -> str | None:
    """Look up the WhatsApp wa_id linked to a user's email, if any."""
    doc = whatsapp_links_collection().find_one({"email": email.lower()})
    return doc.get("waId") if doc else None


def get_first_name_for_email(email: str) -> str:
    """First token of the user's display name, for template greetings. Falls
    back to "there" if the user record or name is missing."""
    doc = users_collection().find_one({"email": email.lower()})
    name = (doc.get("name") if doc else "") or ""
    first = name.strip().split(" ")[0]
    return first or "there"


def is_available() -> bool:
    """True when a connection string is configured (used to gate the scheduler)."""
    return bool(settings.MONGODB_URI)
