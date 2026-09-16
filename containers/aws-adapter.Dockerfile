# Pin by digest in the customer build pipeline (docker buildx imagetools inspect ...).
FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm
WORKDIR /app
COPY requirements-adapter.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY src/adapters/aws_health_issue_adapter.py /app/adapter.py
USER 65532:65532
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/adapter.py"]
