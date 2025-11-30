import base64
import json
import time
import os
from fastapi import FastAPI, Request, HTTPException
from google.cloud import firestore

app = FastAPI()

# Connect to Firestore
# It automatically picks up the 'GOOGLE_APPLICATION_CREDENTIALS' env var
try:
    db = firestore.Client()
except Exception as e:
    print(f"Warning: Firestore client failed to init: {e}")
    db = None

@app.post("/")
async def process_pubsub_message(request: Request):
    """
    Triggered by Google Pub/Sub Pushing a message to us.
    """
    # 1. Decode the Pub/Sub "Envelope"
    # Google sends data wrapped like: {"message": {"data": "base64..."}}
    envelope = await request.json()
    
    if not envelope.get("message"):
        raise HTTPException(status_code=400, detail="Bad Request: no message field")

    pubsub_message = envelope["message"]
    
    # 2. Extract the Data
    try:
        if isinstance(pubsub_message, dict) and "data" in pubsub_message:
            # Data comes as base64 bytes -> decode -> json string -> dict
            data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8")
            payload = json.loads(data_str)
        else:
            raise HTTPException(status_code=400, detail="Invalid message format")
    except Exception as e:
        print(f"Error decoding data: {e}")
        return {"status": "error", "error": str(e)}

    print(f"Received job for tenant: {payload.get('tenant_id')}")

    # 3. CONSTRAINT: Simulate Heavy Processing
    # "Sleep for 0.05s per character"
    text_content = payload.get("text", "")
    sleep_time = len(text_content) * 0.05
    time.sleep(sleep_time) 

    # 4. CONSTRAINT: Multi-Tenant Isolation
    # We must write to: tenants/{tenant_id}/processed_logs/{log_id}
    tenant_id = payload.get("tenant_id")
    log_id = payload.get("log_id")
    
    if db and tenant_id and log_id:
        doc_ref = db.collection("tenants")\
            .document(tenant_id)\
            .collection("processed_logs")\
            .document(log_id)
            
        doc_ref.set({
            "source": payload.get("source"),
            "original_text": text_content,
            "modified_data": text_content.upper(), # <--- Added this to match the PDF example
            "processed_at": firestore.SERVER_TIMESTAMP,
            "char_count": len(text_content)
        })
        print(f"✅ Success: Wrote to Firestore for {tenant_id}")
    else:
        print(f"[LOCAL TEST] Would write to DB for {tenant_id}")

    return {"status": "success"}