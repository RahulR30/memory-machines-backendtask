# Use the official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies directly (Bypassing requirements.txt to avoid file errors)
RUN pip install --no-cache-dir fastapi uvicorn google-cloud-pubsub google-cloud-firestore

# Copy the code
COPY main.py .

# Command to run the API
# We use the port environment variable to ensure it listens where Google wants
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}