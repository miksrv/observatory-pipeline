FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install astap binary
RUN wget -q https://www.hnsky.org/astap/astap_amd64 -O /usr/local/bin/astap \
    && chmod +x /usr/local/bin/astap

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "watcher.py"]
