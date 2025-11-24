FROM python:3.10-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml ./
COPY *.py ./

# Install dependencies
RUN uv sync

# Run the service
CMD ["uv", "run", "main.py"]
