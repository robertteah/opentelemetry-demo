import requests
import time

RELIAI_URL = "http://localhost:8000/api/v1/ingest/traces"
API_KEY = "reliai_7iWP3GxAULTWWYZWKuW1pO0r14zmFud0"


def convert_span(span):
    start = span["startTimeUnixNano"] / 1e6
    end = span["endTimeUnixNano"] / 1e6

    return {
        "trace_id": span["traceId"],
        "span_id": span["spanId"],
        "parent_span_id": span.get("parentSpanId"),
        "timestamp": int(start),
        "latency_ms": int(end - start),
        "success": span.get("status", {}).get("code") == 1,
        "error_type": span.get("status", {}).get("message"),
        "model_name": get_attr(span, "gen_ai.model") or get_attr(span, "llm.model"),
        "service_name": get_resource_attr(span, "service.name"),
    }


def get_attr(span, key):
    for attr in span.get("attributes", []):
        if attr["key"] == key:
            return attr["value"].get("stringValue")
    return None


def get_resource_attr(span, key):
    resource = span.get("resource", {})
    for attr in resource.get("attributes", []):
        if attr["key"] == key:
            return attr["value"].get("stringValue")
    return None


def send_traces(spans):
    payload = {"traces": [convert_span(s) for s in spans]}

    requests.post(
        RELIAI_URL,
        json=payload,
        headers={"x-api-key": API_KEY},
        timeout=5,
    )
