FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install .

COPY Procfile ./

# Create the SQLite/demo-agent persistence dir so the image runs cleanly even
# if no Fly volume is mounted (in production it gets shadowed by the volume).
RUN mkdir -p /data

EXPOSE 8080

CMD ["honcho", "start"]
