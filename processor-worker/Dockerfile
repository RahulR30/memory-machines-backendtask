# Use the official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies directly
RUN pip install --no-cache-dir fastapi uvicorn google-cloud-pubsub google-cloud-firestore

# Copy the code
COPY worker.py .

# Command to run the Worker
CMD exec uvicorn worker:app --host 0.0.0.0 --port ${PORT:-8080}