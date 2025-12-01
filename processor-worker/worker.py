import os
import base64
import json
import time
from fastapi import FastAPI, Request, HTTPException
from google.cloud import firestore

app = FastAPI()

# Initialize Firestore
try:
    db = firestore.Client()
except Exception as e:
    print(f"Warning: Firestore client failed to init: {e}")
    db = None

@app.post("/")
async def process_pubsub_message(request: Request):
    """
    This endpoint receives messages PUSHED from Pub/Sub.
    """
    # Parse the Pub/Sub "Envelope"
    envelope = await request.json()
    
    if not envelope.get("message"):
        raise HTTPException(status_code=400, detail="Bad Request: no message field")

    pubsub_message = envelope["message"]
    
    # 2. Decode the Data
    try:
        # Data comes as base64 bytes -> decode -> json string -> dict
        data_str = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        payload = json.loads(data_str)
    except Exception as e:
        print(f"Error decoding data: {e}")
        return {"status": "data_decode_error"} 

    tenant_id = payload.get("tenant_id")
    log_id = payload.get("log_id")
    text_content = payload.get("text", "")
    char_count = len(text_content)
    
    print(f"Received job for tenant: {tenant_id}, Log ID: {log_id}")

    if "FAIL_FIRST_TIME" in text_content:
        if not os.path.exists("./temporary_data"):
            print("Attempting to crash worker (First run)...")
            os.makedirs("./temporary_data", exist_ok=True) 
            raise ValueError("Intentional crash to test retry mechanism!")
        else:
            print("Resume: Crash marker found, proceeding with job completion.")
            
    # Simulate Heavy Processing
    # Sleep 0.05s per character
    sleep_time = char_count * 0.05
    time.sleep(sleep_time) 

    # Write to Firestore 
    if db and tenant_id and log_id:
        # Multi-Tenant Sub-collection path
        doc_ref = db.collection("tenants")\
            .document(tenant_id)\
            .collection("processed_logs")\
            .document(log_id)
        
        doc_ref.set({
            "source": payload.get("source"),
            "original_text": text_content,
            "modified_data": text_content.upper(),
            "processed_at": firestore.SERVER_TIMESTAMP,
            "char_count": char_count
        })
        print(f"Success: Wrote to Firestore for {tenant_id}")
    else:
        print(f"[ERROR/LOCAL TEST] Failed to write to DB for {tenant_id}")

    return {"status": "success"}
