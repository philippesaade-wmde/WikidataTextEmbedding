FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.7 /uv /uvx /bin/

WORKDIR /workspace

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

# Runtime deps used by /opt/WikidataTextifier modules imported from main.py
RUN uv pip install --python /workspace/.venv/bin/python \
    fastapi==0.116.1 \
    gunicorn==23.0.0 \
    pymysql==1.1.2 \
    rdflib==7.6.0 \
    requests==2.32.4 \
    sqlalchemy==2.0.41 \
    uvicorn==0.35.0

COPY main.py ./main.py
COPY src ./src

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace:/opt

CMD ["/workspace/.venv/bin/python", "main.py"]
