ENRICHED_EVENT_SCHEMA = {
    "type": "object",
    "required": [
        "event_id",
        "user_id",
        "amount",
        "device_type",
        "country",
        "timestamp",
        "risk_score",
        "risk_label",
        "is_high_value"
    ],
    "properties": {
        "event_id": {"type": "string"},
        "user_id": {"type": "string"},
        "amount": {"type": "number"},
        "device_type": {"type": "string"},
        "country": {"type": "string"},
        "timestamp": {"type": "string"},
        "risk_score": {"type": "number"},
        "risk_label": {"type": "string"},
        "is_high_value": {"type": "boolean"}
    },
    "additionalProperties": True
}

ENRICHED_SCHEMA_VERSION = "v2"