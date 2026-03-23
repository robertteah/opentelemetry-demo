FROM python:3.11-slim

WORKDIR /app

COPY requirements.reliai-adapter.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY main.py /app/main.py
COPY reliai_adapter /app/reliai_adapter

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
