"""
Push notification service — sends an FCM notification to every device a
user has registered (web browser or the Android app) when a new briefing is
ready. One unified send path for both platforms: the web client and the
Capacitor plugin both end up producing an FCM registration token, so the
backend never needs to know which platform a given token came from.
"""

import json
import logging

import firebase_admin
from firebase_admin import credentials, messaging

from app.core.config import settings
from app.services.db_service import push_subscriptions_collection
from app.services.notification_log_service import record_notification

logger = logging.getLogger(__name__)

_app: firebase_admin.App | None = None


def _get_app() -> firebase_admin.App:
    global _app
    if _app is None:
        if not settings.FIREBASE_SERVICE_ACCOUNT_JSON:
            raise ValueError("FIREBASE_SERVICE_ACCOUNT_JSON is not configured.")
        cred = credentials.Certificate(json.loads(settings.FIREBASE_SERVICE_ACCOUNT_JSON))
        _app = firebase_admin.initialize_app(cred)
    return _app


def _send_to_active_devices(*, email: str, title: str, body: str, data: dict) -> dict:
    """Shared send path: look up this email's non-disabled subscriptions,
    multicast the notification, and self-heal any tokens FCM reports as
    dead. Used by both the real briefing push and the manual test push."""
    subs = list(push_subscriptions_collection().find({"email": email.lower(), "disabled": {"$ne": True}}))
    if not subs:
        return {"sent": 0, "failed": 0}

    tokens = [s["token"] for s in subs]
    message = messaging.MulticastMessage(
        notification=messaging.Notification(title=title, body=body),
        data=data,
        tokens=tokens,
    )

    response = messaging.send_each_for_multicast(message, app=_get_app())

    # Self-heal: a token FCM reports UNREGISTERED is dead (app uninstalled,
    # browser unsubscribed, token rotated) — without this, every future tick
    # keeps sending to it forever.
    for sub, result in zip(subs, response.responses):
        if not result.success and result.exception is not None:
            code = getattr(result.exception, "code", "")
            if code == "UNREGISTERED":
                push_subscriptions_collection().update_one({"_id": sub["_id"]}, {"$set": {"disabled": True}})

    return {"sent": response.success_count, "failed": response.failure_count}


def send_push_for_briefing(*, email: str, agent_name: str, label: str, briefing_id: str, agent_id: str) -> dict:
    """Notify every device registered for this email that a new briefing is
    ready. Never raises on a partial/total send failure — the caller treats
    this as best-effort and must not let it block the actual delivery."""
    title = agent_name
    body = f"New briefing: {label}"
    data = {"agentId": str(agent_id), "briefingId": str(briefing_id), "click_action": "/dashboard"}

    result = _send_to_active_devices(email=email, title=title, body=body, data=data)
    logger.info(
        "Sent briefing push to %s: %d succeeded, %d failed.",
        email, result["sent"], result.get("failed", 0),
    )

    # Log unconditionally — the in-app inbox should reflect that a briefing
    # notification happened even if FCM delivery failed or found no devices.
    try:
        record_notification(email=email, title=title, body=body, notif_type="briefing", data=data)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to record notification log for %s: %s", email, exc)

    return result


def send_test_push(email: str) -> dict:
    """Back the delivery page's "Send test notification" button. Raises
    ValueError if the user has no active (enabled, non-disabled) device
    to send to, so the route can report a clear 422 instead of a silent
    "sent to nobody"."""
    has_active = push_subscriptions_collection().count_documents(
        {"email": email.lower(), "disabled": {"$ne": True}}
    ) > 0
    if not has_active:
        raise ValueError("No devices have in-app notifications enabled for this account.")

    title = "Leora"
    body = "✅ Test successful — in-app notifications are working."
    data = {"click_action": "/dashboard"}

    result = _send_to_active_devices(email=email, title=title, body=body, data=data)
    logger.info("Sent test push to %s: %d succeeded, %d failed.", email, result["sent"], result.get("failed", 0))

    try:
        record_notification(email=email, title=title, body=body, notif_type="test", data=data)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to record notification log for %s: %s", email, exc)

    return result
