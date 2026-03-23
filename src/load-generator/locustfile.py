#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


import json
import os
import random
import uuid
import logging
import time
import requests

from locust import HttpUser, task, between
from locust_plugins.users.playwright import PlaywrightUser, pw, PageWithRetry, event

from opentelemetry import context, baggage, trace
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.jinja2 import Jinja2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider
from openfeature.contrib.hook.opentelemetry import TracingHook

from playwright.async_api import Route, Request

logger_provider = LoggerProvider(resource=Resource.create(
        {
            "service.name": "load-generator",
        }
    ),)
set_logger_provider(logger_provider)

exporter = OTLPLogExporter(insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)

# Attach OTLP handler to locust logger
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

exporter = OTLPMetricExporter(insecure=True)
set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))

tracer_provider = TracerProvider(
    resource=Resource.create({"service.name": "load-generator"})
)
trace.set_tracer_provider(tracer_provider)
tracer_provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint="http://otel-collector:4318/v1/traces")
    )
)
tracer = trace.get_tracer(__name__)

# Instrumenting manually to avoid error with locust gevent monkey
Jinja2Instrumentor().instrument()
RequestsInstrumentor().instrument()
SystemMetricsInstrumentor().instrument()
URLLib3Instrumentor().instrument()
logging.info("Instrumentation complete")

# Initialize Flagd provider
api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
api.add_hooks([TracingHook()])

def get_flagd_value(FlagName):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_integer_value(FlagName, 0)

categories = [
    "binoculars",
    "telescopes",
    "accessories",
    "assembly",
    "travel",
    "books",
    None,
]

products = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
    "HQTGWGPNH4",
]

people_file = open('people.json')
people = json.load(people_file)

class WebsiteUser(HttpUser):
    wait_time = between(1, 10)

    def _traced_request(self, method: str, path: str, **kwargs):
        base_url = self.host or os.environ.get("LOCUST_HOST", "")
        if base_url and path.startswith("/"):
            url = f"{base_url}{path}"
        else:
            url = path
        headers = kwargs.pop("headers", {}) or {}
        inject(headers)
        return requests.request(method, url, headers=headers, timeout=5, **kwargs)

    def run_retrieval_flow(self):
        print("EMITTING RETRIEVAL TRACE")
        with tracer.start_as_current_span("retrieval.request") as parent:
            parent.set_attribute("span_type", "retrieval")

            for attempt in range(1, 3):
                with tracer.start_as_current_span("retrieval.attempt") as span:
                    span.set_attribute("retry_attempt", attempt)
                    span.set_attribute("span_type", "retrieval")
                    span.set_attribute("vector_store", "pgvector")
                    span.set_attribute("query_type", "semantic_search")

                    if attempt == 1:
                        span.set_attribute("failure_reason", "stale_context")
                        span.set_attribute("documents_found", 0)
                        span.set_attribute(
                            "explanation",
                            "Retriever returned no relevant documents",
                        )
                        span.record_exception(Exception("retrieval failed"))
                        span.set_status(Status(StatusCode.ERROR))
                    else:
                        span.set_attribute("documents_found", 3)
                        span.set_attribute(
                            "explanation",
                            "Retry returned relevant documents",
                        )
                        span.set_status(Status(StatusCode.OK))

    @task(1)
    def index(self):
        self.client.get("/")

    @task(1)
    def wow_trace(self):
        with tracer.start_as_current_span("request.root") as root:
            root.set_attribute("span_type", "request")
            root.set_attribute("scenario", "retrieval_retry")
            self.run_retrieval_flow()
            with tracer.start_as_current_span("guardrail_check") as guardrail:
                guardrail.set_attribute("span_type", "guardrail")
                guardrail.set_attribute("policy", "retrieval_safety")
                guardrail.set_status(Status(StatusCode.OK))
            with tracer.start_as_current_span("tool_router") as tool_router:
                tool_router.set_attribute("span_type", "tool")
                tool_router.set_attribute("tool", "context_enricher")
                tool_router.set_status(Status(StatusCode.OK))
            self._traced_request(
                "GET",
                "/api/recommendations",
                params={"productIds": [random.choice(products)]},
            )
            self._traced_request(
                "GET",
                "/api/data/",
                params={"contextKeys": [random.choice(categories)]},
            )

    @task(10)
    def browse_product(self):
        self.client.get("/api/products/" + random.choice(products))

    @task(3)
    def get_recommendations(self):
        params = {
            "productIds": [random.choice(products)],
        }
        self.client.get("/api/recommendations", params=params)

    @task(3)
    def get_ads(self):
        params = {
            "contextKeys": [random.choice(categories)],
        }
        self.client.get("/api/data/", params=params)

    @task(3)
    def view_cart(self):
        self.client.get("/api/cart")

    @task(2)
    def add_to_cart(self, user=""):
        if user == "":
            user = str(uuid.uuid1())
        product = random.choice(products)
        self.client.get("/api/products/" + product)
        cart_item = {
            "item": {
                "productId": product,
                "quantity": random.choice([1, 2, 3, 4, 5, 10]),
            },
            "userId": user,
        }
        self.client.post("/api/cart", json=cart_item)

    @task(1)
    def checkout(self):
        # checkout call with an item added to cart
        user = str(uuid.uuid1())
        self.add_to_cart(user=user)
        checkout_person = random.choice(people)
        checkout_person["userId"] = user
        self.client.post("/api/checkout", json=checkout_person)

    @task(1)
    def checkout_multi(self):
        # checkout call which adds 2-4 different items to cart before checkout
        user = str(uuid.uuid1())
        for i in range(random.choice([2, 3, 4])):
            self.add_to_cart(user=user)
        checkout_person = random.choice(people)
        checkout_person["userId"] = user
        self.client.post("/api/checkout", json=checkout_person)

    @task(5)
    def flood_home(self):
        for _ in range(0, get_flagd_value("loadGeneratorFloodHomepage")):
            self.client.get("/")

    def on_start(self):
        ctx = baggage.set_baggage("session.id", str(uuid.uuid4()))
        ctx = baggage.set_baggage("synthetic_request", "true", context=ctx)
        context.attach(ctx)
        self.index()


browser_traffic_enabled = os.environ.get("LOCUST_BROWSER_TRAFFIC_ENABLED", "").lower() in ("true", "yes", "on")

if browser_traffic_enabled:
    class WebsiteBrowserUser(PlaywrightUser):
        headless = True  # to use a headless browser, without a GUI

        @task
        @pw
        async def open_cart_page_and_change_currency(self, page: PageWithRetry):
            try:
                page.on("console", lambda msg: print(msg.text))
                await page.route('**/*', add_baggage_header)
                await page.goto("/cart", wait_until="domcontentloaded")
                await page.select_option('[name="currency_code"]', 'CHF')
                await page.wait_for_timeout(2000)  # giving the browser time to export the traces
            except:
                pass

        @task
        @pw
        async def add_product_to_cart(self, page: PageWithRetry):
            try:
                page.on("console", lambda msg: print(msg.text))
                await page.route('**/*', add_baggage_header)
                await page.goto("/", wait_until="domcontentloaded")
                await page.click('p:has-text("Roof Binoculars")', wait_until="domcontentloaded")
                await page.click('button:has-text("Add To Cart")', wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)  # giving the browser time to export the traces
            except:
                pass


async def add_baggage_header(route: Route, request: Request):
    existing_baggage = request.headers.get('baggage', '')
    headers = {
        **request.headers,
        'baggage': ', '.join(filter(None, (existing_baggage, 'synthetic_request=true')))
    }
    await route.continue_(headers=headers)
