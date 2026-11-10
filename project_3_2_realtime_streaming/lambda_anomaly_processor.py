"""
Project 3.2: Real-Time Streaming Analytics Pipeline
lambda_anomaly_processor.py - Decodes Kinesis base64 records, flags anomalies & indexes to OpenSearch.
Triggered automatically by Kinesis Data Streams event source mapping.
"""

import os
import base64
import json
import urllib3

http = urllib3.PoolManager()
OPENSEARCH_URL = os.environ.get("OPENSEARCH_ENDPOINT") or os.environ.get("OPENSEARCH_HOST", "")
HEADERS = {"Content-Type": "application/json"}


def lambda_handler(event, context):
    """
    Decodes each record from Kinesis event, evaluates anomaly rules (> 90.0°C),
    and indexes enriched JSON documents directly into Amazon OpenSearch Service.
    """
    processed, anomalies = 0, 0
    for record in event.get("Records", []):
        raw_bytes = base64.b64decode(record["kinesis"]["data"])
        doc = json.loads(raw_bytes.decode("utf-8"))
        doc["is_anomaly"] = bool(doc.get("temperature", 0.0) > 90.0)
        if doc["is_anomaly"]:
            anomalies += 1
            print(f"CRITICAL ANOMALY DETECTED: {json.dumps(doc)}")
        if OPENSEARCH_URL:
            try:
                http.request(
                    "POST",
                    f"{OPENSEARCH_URL}/iot-metrics/_doc",
                    body=json.dumps(doc),
                    headers=HEADERS
                )
            except Exception as e:
                print(f"Failed to post metric to OpenSearch: {e}")
        processed += 1
    return {"statusCode": 200, "processed": processed, "anomalies": anomalies}
