FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
# yt-dlp needs a JS runtime for YouTube (since 2025.11.12); yt-dlp picks up deno automatically.
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno
RUN useradd -m app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=app . .
USER app
ENV PYTHONUNBUFFERED=1
HEALTHCHECK --interval=60s --timeout=5s CMD python -c "import urllib.request,os;urllib.request.urlopen('http://127.0.0.1:%s/health'%os.getenv('PORT','8000'))"
# Upgrade yt-dlp on every start (sites change often); main.py also upgrades daily.
CMD ["sh","-c","pip install --user -q -U 'yt-dlp[default]' || true; exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips='*'"]
