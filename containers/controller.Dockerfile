# Pin by digest in the customer build pipeline (docker buildx imagetools inspect ...).
FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm
WORKDIR /app
COPY src/controller/remediation_controller.py /app/controller.py
USER 65532:65532
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/controller.py"]
