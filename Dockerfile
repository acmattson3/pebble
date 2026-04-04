FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libgl1 \
    libgomp1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./web-interface/requirements.txt /tmp/web-requirements.txt
RUN pip install --no-cache-dir -r /tmp/web-requirements.txt pyserial

COPY . /app

EXPOSE 8080

CMD ["python3", "-u", "control/launcher.py", "--config", "control/configs/config.json"]
