FROM python:3.11-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY mock_lab ./mock_lab
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv \
    QC_DB_PATH=/data/qc.db

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
