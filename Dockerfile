FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir .[dev]

RUN chmod +x scripts/start.sh

CMD ["./scripts/start.sh"]
