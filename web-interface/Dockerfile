FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libgl1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./web-interface/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./web-interface /app/web-interface
COPY ./control/configs /app/control/configs

WORKDIR /app/web-interface

EXPOSE 8080

CMD ["python3", "-u", "web-control.py"]
