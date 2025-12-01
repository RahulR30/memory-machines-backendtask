import os
import json
import base64
from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from google.cloud import pubsub_v1

app = FastAPI()

# Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id-here")
TOPIC_ID = "ingestion-topic"

# Initialize Pub/Sub Publisher
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
except Exception as e:
    print(f"Warning: Pub/Sub client failed to init (expected if local): {e}")
    publisher = None

class LogPayload(BaseModel):
    tenant_id: str
    log_id: str
    text: str

@app.post("/ingest", status_code=202)
async def ingest_data(
    request: Request, 
    x_tenant_id: Optional[str] = Header(None) # Extract header for TXT 
):
    content_type = request.headers.get("content-type", "")
    
    final_payload = {}
    
    if "application/json" in content_type:
        # JSON Payload
        try:
            body = await request.json()
            if "tenant_id" not in body or "text" not in body:
                raise HTTPException(status_code=400, detail="Missing tenant_id or text in JSON")
            
            final_payload = {
                "tenant_id": body["tenant_id"],
                "log_id": body.get("log_id", "generated-id"),
                "text": body["text"],
                "source": "json_upload"
            }
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")

    elif "text/plain" in content_type:
        # Unstructured Text
        if not x_tenant_id:
            raise HTTPException(status_code=400, detail="X-Tenant-ID header required for text")
        
        body_bytes = await request.body()
        text_content = body_bytes.decode("utf-8")
        
        final_payload = {
            "tenant_id": x_tenant_id,
            "log_id": "generated-log-id",
            "text": text_content,
            "source": "text_upload"
        }
        
    else:
        raise HTTPException(status_code=415, detail="Unsupported Content-Type")

    # We serialize the normalized data to JSON bytes
    data_str = json.dumps(final_payload)
    data_bytes = data_str.encode("utf-8")

    if publisher:
        try:
            # Publish the message
            future = publisher.publish(topic_path, data_bytes)
            message_id = future.result()
            print(f"Published message {message_id}")
        except Exception as e:
            print(f"Publishing failed: {e}")
    else:
        print(f"[LOCAL TEST] Would publish: {final_payload}")

    return {"status": "accepted", "message": "Log queued for processing"}

# To run: uvicorn main:app --reload