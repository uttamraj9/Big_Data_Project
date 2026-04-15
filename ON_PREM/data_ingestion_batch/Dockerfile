# Start from the official Python slim image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install required system libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ETL script, and any other source files
COPY src ./src

# Copy data file 
COPY data ./data

# Define default env vars (unbuffered logging)
ENV PYTHONUNBUFFERED=1

# Default command can be overridden with `docker run <imageName> full`
ENTRYPOINT ["python", "src/etl_to_postgres.py"]
