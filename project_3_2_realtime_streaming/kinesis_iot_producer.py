"""
Project 3.2: Real-Time Streaming Analytics Pipeline
kinesis_iot_producer.py - Live telemetry stream generator (IoT sensors + clickstreams).
Streams simulated IoT sensor readings into Amazon Kinesis Data Streams and injects high-temperature anomaly spikes (> 90.0°C).
"""

import os
import boto3
import json
import random
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError


def create_kinesis_stream(stream_name: str = "iot-sensor-stream", shard_count: int = 1, region: str = "us-east-1"):
    """
    Provisions Amazon Kinesis Data Stream with the specified shard count.
    """
    kinesis = boto3.client("kinesis", region_name=region)
    try:
        kinesis.create_stream(StreamName=stream_name, ShardCount=shard_count)
        print(f"Provisioning Kinesis stream '{stream_name}' (1 Shard)...")
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)
        print(f"Kinesis stream '{stream_name}' is ACTIVE.")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceInUseException":
            print(f"Kinesis stream '{stream_name}' already exists and is in use.")
        else:
            print(f"Notice creating Kinesis stream: {e}")


def stream_iot_events(stream_name: str = "iot-sensor-stream", count: int = 50):
    """
    Streams simulated IoT sensor readings into Amazon Kinesis Data Streams
    and injects high-temperature anomaly spikes (> 90.0°C) for real-time detection.
    """
    kinesis = boto3.client("kinesis")
    print(f"Streaming {count} IoT events to Kinesis stream '{stream_name}'...")
    for i in range(count):
        payload = {
            "sensor_id": f"SENSOR_{random.randint(1, 5)}",
            "temperature": round(random.uniform(22.0, 105.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(payload).encode("utf-8"),
            PartitionKey=payload["sensor_id"]
        )
        if payload["temperature"] > 90.0:
            print(f"  [{i+1}/{count}] Emitted SPIKE: {payload['sensor_id']} @ {payload['temperature']}°C")
        else:
            print(f"  [{i+1}/{count}] Emitted metric: {payload['sensor_id']} @ {payload['temperature']}°C")
        time.sleep(0.2)
    print("Stream production finished.")


if __name__ == "__main__":
    stream = os.environ.get("KINESIS_STREAM", "iot-sensor-stream")
    region = os.environ.get("AWS_REGION", "us-east-1")
    create_kinesis_stream(stream, 1, region)
    stream_iot_events(stream, 50)
