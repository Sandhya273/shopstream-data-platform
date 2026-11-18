# dbt + DQ runner image (ECS Fargate task launched by Step Functions)
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/app/dbt
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt dbt-snowflake==1.10.* snowflake-connector-python cryptography
COPY dbt/ dbt/
COPY data_quality/ data_quality/
COPY ingestion/common/ ingestion/common/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && useradd -m runner && chown -R runner /app
USER runner
WORKDIR /app/dbt
RUN dbt deps || true
ENTRYPOINT ["/entrypoint.sh"]
CMD ["dbt", "build", "--target", "prod"]
