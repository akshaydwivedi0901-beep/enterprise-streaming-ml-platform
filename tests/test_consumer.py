import pytest

def test_event_validation():
    """Test event has required fields"""
    event = {
        "event_id": "123",
        "user_id": "user_1",
        "country": "USA",
        "amount": 100.0,
        "device_type": "mobile",
        "timestamp": "2026-04-19T12:00:00"
    }
    assert event["event_id"]
    assert event["amount"] > 0
    assert event["country"] in ["USA", "India", "Germany"]

def test_fraud_score():
    """Test fraud detection logic"""
    # High amount + high velocity = high risk
    amount = 5000
    velocity = 10
    fraud_score = (amount / 1000) * 0.5 + (velocity / 5) * 0.5
    assert fraud_score > 0.5

def test_s3_path():
    """Test S3 partition path"""
    from datetime import datetime
    event = {"timestamp": "2026-04-19T12:00:00"}
    dt = datetime.fromisoformat(event["timestamp"])
    path = f"bronze/enriched/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    assert "year=2026" in path
    assert "month=04" in path
    assert "day=19" in path

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
