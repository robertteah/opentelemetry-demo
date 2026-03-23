FROM python:3.12-slim-bookworm AS base

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

RUN pip install --upgrade pip setuptools
RUN pip install -r requirements.txt

COPY . .

RUN python -m pip install --no-cache-dir "setuptools<81" \
    && opentelemetry-bootstrap -a install 

ENV RECOMMENDATION_PORT 1010

ENTRYPOINT ["python", "recommendation_server.py"]
