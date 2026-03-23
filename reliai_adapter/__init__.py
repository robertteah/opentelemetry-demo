import os
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx
from opentelemetry.proto.trace.v1.trace_pb2 import Span as OTelSpan
from opentelemetry.proto.resource.v1.resource_pb2 import Resource as OTelResource
from opentelemetry.proto.common.v1.common_pb2 import AnyValue as OTelAnyValue

RELIAI_API_URL = os.getenv("RELIAI_API_URL", "http://localhost:8000").rstrip("/")
RELIAI_API_KEY = os.getenv("RELIAI_API_KEY")
RELIAI_ENV = os.getenv("RELIAI_ENV", "prod")
RELIAI_PROJECT_ID = os.getenv("RELIAI_PROJECT_ID")


def _nanos_to_datetime(nanos: int | str | None) -> datetime:
    if nanos is None:
        return datetime.now(timezone.utc)
    try:
        value = int(nanos)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)


def _duration_ms(start_ns: int | str | None, end_ns: int | str | None) -> int | None:
    try:
        start_val = int(start_ns)
        end_val = int(end_ns)
    except (TypeError, ValueError):
        return None
    if end_val < start_val:
        return None
    return int((end_val - start_val) / 1_000_000)


def _otel_value(value: dict[str, Any]) -> Any:
    if "stringValue" in value:
        return value["stringValue"]
    if "intValue" in value:
        return value["intValue"]
    if "doubleValue" in value:
        return value["doubleValue"]
    if "boolValue" in value:
        return value["boolValue"]
    if "arrayValue" in value:
        return [_otel_value(item) for item in value.get("arrayValue", {}).get("values", [])]
    return value


def resource_attrs(resource_span: dict[str, Any]) -> dict[str, Any]:
    resource = resource_span.get("resource") or {}
    attrs = {}
    for item in resource.get("attributes", []) or []:
        key = item.get("key")
        value = item.get("value", {})
        if not key:
            continue
        attrs[key] = _otel_value(value)
    return attrs


def _span_attrs(span: dict[str, Any]) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    for item in span.get("attributes", []) or []:
        key = item.get("key")
        value = item.get("value", {})
        if not key:
            continue
        attrs[key] = _otel_value(value)
    return attrs


def _status_info(span: dict[str, Any]) -> tuple[bool, str | None]:
    status = span.get("status") or {}
    code = status.get("code")
    message = status.get("message")
    success = code != 2
    return success, message


def _truncate_error(message: str | None, limit: int = 120) -> str | None:
    if not message:
        return None
    if len(message) <= limit:
        return message
    return message[: limit - 1] + "…"


def _bytes_to_hex(value: bytes | None) -> str | None:
    if not value:
        return None
    return value.hex()


def _proto_value(value: OTelAnyValue | None) -> Any:
    if value is None:
        return None
    which = value.WhichOneof("value")
    if which == "string_value":
        return value.string_value
    if which == "int_value":
        return value.int_value
    if which == "double_value":
        return value.double_value
    if which == "bool_value":
        return value.bool_value
    if which == "array_value":
        return [_proto_value(item) for item in value.array_value.values]
    if which == "kvlist_value":
        return {item.key: _proto_value(item.value) for item in value.kvlist_value.values}
    return None


def resource_attrs_proto(resource: OTelResource | None) -> dict[str, Any]:
    if resource is None:
        return {}
    attrs: dict[str, Any] = {}
    for item in resource.attributes:
        if not item.key:
            continue
        attrs[item.key] = _proto_value(item.value)
    return attrs


def _span_attrs_proto(span: OTelSpan) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    for item in span.attributes:
        if not item.key:
            continue
        attrs[item.key] = _proto_value(item.value)
    return attrs


def _status_info_proto(span: OTelSpan) -> tuple[bool, str | None]:
    status = span.status
    code = status.code
    message = status.message or None
    success = code != 2
    return success, message


def _build_payload(span: dict[str, Any], resource: dict[str, Any]) -> dict[str, Any]:
    trace_id = span.get("traceId") or span.get("trace_id")
    span_id = span.get("spanId") or span.get("span_id")
    parent_span_id = span.get("parentSpanId") or span.get("parent_span_id")
    start_ns = span.get("startTimeUnixNano")
    end_ns = span.get("endTimeUnixNano")

    success, error_message = _status_info(span)
    error_message = _truncate_error(error_message)

    span_attributes = _span_attrs(span)
    model_name = span_attributes.get("gen_ai.model") or span_attributes.get("llm.model") or "otel"

    payload = {
        "timestamp": _nanos_to_datetime(start_ns).isoformat(),
        "request_id": span_id or "otel-span",
        "service_name": resource.get("service.name"),
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "span_name": span.get("name"),
        "model_name": str(model_name),
        "model_provider": span_attributes.get("llm.provider"),
        "latency_ms": _duration_ms(start_ns, end_ns),
        "success": bool(success),
        "error_type": None if success else (error_message or "otel_error"),
        "environment": RELIAI_ENV,
        "metadata_json": {
            "otel": {
                "kind": span.get("kind"),
                "attributes": span_attributes,
            }
        },
    }
    if RELIAI_PROJECT_ID:
        payload["project_id"] = RELIAI_PROJECT_ID
    return payload


def _build_payload_proto(span: OTelSpan, resource: dict[str, Any]) -> dict[str, Any]:
    trace_id = _bytes_to_hex(span.trace_id)
    span_id = _bytes_to_hex(span.span_id)
    parent_span_id = _bytes_to_hex(span.parent_span_id)
    start_ns = span.start_time_unix_nano
    end_ns = span.end_time_unix_nano

    success, error_message = _status_info_proto(span)
    error_message = _truncate_error(error_message)
    span_attributes = _span_attrs_proto(span)
    model_name = span_attributes.get("gen_ai.model") or span_attributes.get("llm.model") or "otel"

    payload = {
        "timestamp": _nanos_to_datetime(start_ns).isoformat(),
        "request_id": span_id or "otel-span",
        "service_name": resource.get("service.name"),
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "span_name": span.name,
        "model_name": str(model_name),
        "model_provider": span_attributes.get("llm.provider"),
        "latency_ms": _duration_ms(start_ns, end_ns),
        "success": bool(success),
        "error_type": None if success else (error_message or "otel_error"),
        "environment": RELIAI_ENV,
        "metadata_json": {
            "otel": {
                "kind": span.kind,
                "attributes": span_attributes,
            }
        },
    }
    if RELIAI_PROJECT_ID:
        payload["project_id"] = RELIAI_PROJECT_ID
    return payload


def send_traces(spans: Iterable[dict[str, Any]], resource: dict[str, Any]) -> None:
    if not RELIAI_API_KEY:
        raise RuntimeError("RELIAI_API_KEY is required")
    endpoint = f"{RELIAI_API_URL}/api/v1/ingest/traces"
    headers = {"x-api-key": RELIAI_API_KEY, "Content-Type": "application/json"}
    with httpx.Client(timeout=5) as client:
        for span in spans:
            payload = _build_payload(span, resource)
            resp = client.post(endpoint, headers=headers, json=payload)
            if resp.status_code >= 400:
                print(f"reliai ingest failed: {resp.status_code} {resp.text[:200]}")


def send_traces_proto(spans: Iterable[OTelSpan], resource: dict[str, Any]) -> None:
    if not RELIAI_API_KEY:
        raise RuntimeError("RELIAI_API_KEY is required")
    endpoint = f"{RELIAI_API_URL}/api/v1/ingest/traces"
    headers = {"x-api-key": RELIAI_API_KEY, "Content-Type": "application/json"}
    with httpx.Client(timeout=5) as client:
        for span in spans:
            payload = _build_payload_proto(span, resource)
            resp = client.post(endpoint, headers=headers, json=payload)
            if resp.status_code >= 400:
                print(f"reliai ingest failed: {resp.status_code} {resp.text[:200]}")
