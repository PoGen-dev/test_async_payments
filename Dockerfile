FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /service
COPY pyproject.toml ./
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
RUN pip install --no-cache-dir .
RUN useradd --system --uid 10001 appuser
USER appuser
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
