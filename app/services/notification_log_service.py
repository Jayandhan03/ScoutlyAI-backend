"""
Notification log service — permanent in-app record of push notifications.

Written every time push_service builds a notification (briefing-ready or
test), independent of whether FCM actually delivered it — this is an in-app
inbox, not a delivery receipt. Mirrored on the frontend by
models/NotificationLog.ts, which is what the bell dropdown reads/mutates.
"""

import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from app.services.db_service import notification_logs_collection

logger = logging.getLogger(__name__)


def record_notification(
    *, email: str, title: str, body: str, notif_type: str, data: dict[str, Any] | None = None
) -> ObjectId:
    """Insert one notification log doc. Callers should treat this as
    best-effort (wrap in try/except) — a logging failure must never block an
    actual push send."""
    doc: dict[str, Any] = {
        "email": email.lower(),
        "title": title,
        "body": body,
        "type": notif_type,
        "data": data or {},
        "read": False,
        "readAt": None,
        "createdAt": datetime.now(timezone.utc),
    }
    log_id = notification_logs_collection().insert_one(doc).inserted_id
    logger.info("Recorded %s notification log for %s.", notif_type, email)
    return log_id
