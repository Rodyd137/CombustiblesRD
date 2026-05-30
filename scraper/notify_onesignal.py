"""
notify_onesignal.py
====================

Reads the freshly-published `data/latest.json` and, if the run resulted in
real price changes (any item has a non-null `change` block), fires a single
push notification to all OneSignal subscribers of the Bartelo app.

Configuration is read from env vars (set in the GitHub workflow):
    ONESIGNAL_APP_ID         (required)  - the Bartelo OneSignal app id
    ONESIGNAL_REST_API_KEY   (required)  - Settings → Keys & IDs → REST API Key

If either is missing OR if no items changed, this script exits 0 silently
so the workflow keeps green.
"""

import json
import os
import sys
import urllib.request
import urllib.error


LATEST_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "latest.json")


def _format_summary(items):
    """Build a short Spanish notification body summarising up/down moves."""
    changes = [it for it in items if it.get("change")]
    if not changes:
        return None

    ups = sum(1 for c in changes if c["change"].get("type") == "up")
    downs = sum(1 for c in changes if c["change"].get("type") == "down")
    sames = sum(1 for c in changes if c["change"].get("type") == "same")

    # Only worth pushing when at least one price actually moved.
    if not (ups or downs):
        return None

    parts = []
    if ups:   parts.append(f"⬆️ {ups} sube{'n' if ups>1 else ''}")
    if downs: parts.append(f"⬇️ {downs} baja{'n' if downs>1 else ''}")

    # Most-moved product for headline color
    top = max(
        changes,
        key=lambda it: abs(it["change"].get("amount_dop") or 0),
    )
    top_label = top.get("label", "Combustible")
    top_amt = top["change"].get("amount_dop") or 0
    top_dir = "+" if top_amt > 0 else ""
    headline = f"{', '.join(parts)} · {top_label} {top_dir}{top_amt:.2f}"
    return headline


def main():
    app_id = os.environ.get("ONESIGNAL_APP_ID", "").strip()
    api_key = os.environ.get("ONESIGNAL_REST_API_KEY", "").strip()
    if not app_id or not api_key:
        print("ℹ️  ONESIGNAL_APP_ID / ONESIGNAL_REST_API_KEY not set — skipping push.")
        return 0

    if not os.path.exists(LATEST_PATH):
        print("⚠️  data/latest.json not found — nothing to notify about.")
        return 0

    with open(LATEST_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    items = payload.get("items") or []
    summary = _format_summary(items)
    if not summary:
        print("ℹ️  No items reported a `change` (probably first-publish or no-op). Skipping push.")
        return 0

    week_label = payload.get("week_label") or "Nuevos precios disponibles"

    body = {
        "app_id": app_id,
        "included_segments": ["Subscribed Users"],
        "headings": {"en": "⛽ Combustibles RD", "es": "⛽ Combustibles RD"},
        "contents": {"en": summary, "es": summary},
        "subtitle": {"en": week_label, "es": week_label},
        "data": {
            "type": "fuel_update",
            "week_label": week_label,
        },
        # Open the Fuel tab when tapped (deep link)
        "url": "bartelo://explore/fuel",
        "ios_badgeType": "Increase",
        "ios_badgeCount": 1,
    }

    req = urllib.request.Request(
        "https://api.onesignal.com/notifications",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Basic {api_key}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            print(f"✅ OneSignal accepted (HTTP {resp.status}). Body: {raw[:300]}")
            return 0
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        print(f"⚠️  OneSignal HTTP {e.code}: {err[:500]}")
        # Don't fail the workflow on push errors — the data publish is the
        # primary contract.
        return 0
    except Exception as e:
        print(f"⚠️  OneSignal request failed: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
