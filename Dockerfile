FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    file \
    libgtk-3-0 \
    libharfbuzz-gobject0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install astap binary from local archive
# Download manually from: https://sourceforge.net/projects/astap-program/files/linux_installer/
# Place the appropriate tar.gz for your architecture in install/ directory
COPY install/astap_*.tar.gz /tmp/
RUN tar -xzf /tmp/astap_*.tar.gz -C / && \
    chmod +x /opt/astap/astap && \
    rm -rf /tmp/astap*.tar.gz && \
    echo "SUCCESS: astap binary installed at $(readlink -f /usr/local/bin/astap)"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "watcher.py"]
