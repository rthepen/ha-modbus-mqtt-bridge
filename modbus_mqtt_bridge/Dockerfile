ARG BUILD_FROM=ghcr.io/home-assistant/alpine-base:3.19
FROM ${BUILD_FROM}

# Install dependencies
RUN apk add --no-cache python3 py3-pip py3-yaml

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages

# Copy code
COPY bridge.py .
COPY run.sh .

RUN chmod +x run.sh

CMD [ "./run.sh" ]
