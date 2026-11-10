# The fraud scorer, the simulator and training: one image.
#   docker build -t ledgerline-fraud fraud
FROM python:3.13.15-slim
# LightGBM needs OpenMP.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 fraud && mkdir /model && chown 10001 /model
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fraud ./fraud
COPY tests ./tests
USER 10001
ENV PYTHONUNBUFFERED=1 MODEL_DIR=/model
EXPOSE 8000
CMD ["uvicorn", "fraud.service:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
