FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/ ./pipeline/
COPY manifests/ ./manifests/
COPY source/ ./source/
COPY run.py .

CMD ["python", "run.py", "manifests/health_plan_b.yaml"]