import json

def test_alerts_json_valid():
    """Validate alerts.json is parseable and structured."""
    with open("alerts/alerts.json", "r") as f:
        alerts = json.load(f)

    assert isinstance(alerts, list)
    for alert in alerts:
        assert "name" in alert
        assert "condition" in alert
        assert "severity" in alert
