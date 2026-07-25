# SUPRIME swarm node image.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY suprime ./suprime
RUN pip install --no-cache-dir -e .

# Default gossip/TCP port; override with SUPRIME_PORT.
EXPOSE 7000

# All configuration comes from SUPRIME_* environment variables (see suprime/config.py).
CMD ["python", "-m", "suprime", "serve"]
