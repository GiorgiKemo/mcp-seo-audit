FROM python:3.13-slim

WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY pyproject.toml README.md LICENSE gsc_server.py seo_*.py ./
RUN pip install --no-cache-dir --no-deps . \
    && useradd --create-home --uid 10001 auditor \
    && mkdir /data && chown auditor:auditor /data

ENV GSC_SKIP_OAUTH=true \
    SEO_AUDIT_ENABLE_WRITE_TOOLS=false \
    SEO_AUDIT_ALLOW_PRIVATE_URLS=false \
    SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=false \
    SEO_AUDIT_DATA_DIR=/data \
    PYTHONUNBUFFERED=1

USER auditor
VOLUME ["/data"]
ENTRYPOINT ["mcp-seo-audit"]
