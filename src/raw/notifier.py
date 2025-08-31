import requests

def send_notification(dbutils, msg, config):
    ncfg = config.get("notifications", {})
    ntype = ncfg.get("type", "none")
    webhook_secret = ncfg.get("webhook_secret")

    if ntype == "none":
        print("ℹ️ Notifications disabled.")
        return

    if not webhook_secret:
        print("⚠️ No webhook configured.")
        return

    webhook_url = dbutils.secrets.get(scope="notify-secrets", key=webhook_secret)

    if ntype == "slack":
        payload = {"text": msg}
        requests.post(webhook_url, json=payload)

    elif ntype == "teams":
        payload = {"text": msg}
        requests.post(webhook_url, json=payload)

    elif ntype == "email":
        # Placeholder: integrate with SendGrid / SMTP
        recipients = ", ".join(ncfg.get("email_recipients", []))
        print(f"📧 Email would be sent to {recipients}: {msg}")

    print(f"✅ Notification sent via {ntype}")
