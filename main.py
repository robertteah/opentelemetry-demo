import gzip
import json

from fastapi import FastAPI, Request
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from reliai_adapter import resource_attrs, resource_attrs_proto, send_traces, send_traces_proto

app = FastAPI()


@app.post("/otlp")
@app.post("/otlp/v1/traces")
@app.post("/v1/traces")
async def receive_otlp(req: Request):
    body = await req.body()
    if not body:
        return {"status": "empty"}

    content_type = req.headers.get("content-type", "")
    encoding = req.headers.get("content-encoding", "")

    if "gzip" in encoding:
        body = gzip.decompress(body)

    if "application/json" in content_type or body.lstrip().startswith(b"{"):
        try:
            data = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict):
            for resource_span in data.get("resourceSpans", []):
                attrs = resource_attrs(resource_span)
                for scope_span in resource_span.get("scopeSpans", []):
                    spans = scope_span.get("spans", [])
                    if spans:
                        send_traces(spans, attrs)
            return {"status": "ok"}

    request = ExportTraceServiceRequest()
    request.ParseFromString(body)
    for resource_span in request.resource_spans:
        attrs = resource_attrs_proto(resource_span.resource)
        for scope_span in resource_span.scope_spans:
            spans = scope_span.spans
            if spans:
                send_traces_proto(spans, attrs)
    return {"status": "ok"}
