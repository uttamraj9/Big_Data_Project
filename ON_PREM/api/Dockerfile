# Start from the official Python slim image
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the src folder code
COPY src/ .


# Expose the port FastAPI will run on
EXPOSE 5310

# Define default env vars (just in case)
ENV PYTHONUNBUFFERED=1

# Launch Uvicorn
CMD ["uvicorn", "stream_api:app", "--host", "0.0.0.0", "--port", "5310"]
