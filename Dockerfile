FROM python:3.12-slim

WORKDIR /app/src

RUN pip install poetry \
    && apt-get update \
    && apt-get install -y build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock* ../
RUN poetry config virtualenvs.create false && poetry install --no-root

COPY . ../

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]