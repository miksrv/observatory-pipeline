FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libcfitsio-dev \
    file \
    libgtk-3-0 \
    libgtk2.0-0 \
    libharfbuzz-gobject0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Install astap binary from local archive
# Downloaded from: https://sourceforge.net/projects/astap-program/files/linux_installer/
# install/ holds one archive per architecture; the one matching the image's
# platform is picked (amd64 servers, arm64 Apple Silicon / Raspberry Pi).
COPY install/astap_*.tar.gz /tmp/
RUN case "$(dpkg --print-architecture)" in \
        amd64) ASTAP_ARCHIVE=astap_amd64.tar.gz ;; \
        arm64) ASTAP_ARCHIVE=astap_aarch64.tar.gz ;; \
        *) echo "ERROR: no astap archive for $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac && \
    tar -xzf "/tmp/$ASTAP_ARCHIVE" -C / && \
    chmod +x /opt/astap/astap && \
    mkdir -p /usr/local/bin && \
    ln -sf /opt/astap/astap /usr/local/bin/astap && \
    rm -rf /tmp/astap*.tar.gz && \
    echo "SUCCESS: astap binary installed at $(readlink -f /usr/local/bin/astap)"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "watcher.py"]
