# Multi-stage: minimal prod image
FROM python:3.11-slim as builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir pyspark delta-spark pyyaml requests

FROM python:3.11-slim
WORKDIR /app

# Copy only installed packages from builder
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/

# Verify
RUN python -c "from src.bronze import BronzeIngestion; print('Bronze loaded ✅')" && \
    python -c "from src.silver import SilverTransformer; print('Silver loaded ✅')" && \
    python -c "from src.gold import GoldAggregator; print('Gold loaded ✅')" && \
    python -c "from src.quality import DataQuality; print('Quality loaded ✅')"

ENTRYPOINT ["python", "-m", "pytest", "tests/", "-v"]
