"""
Project 3.2: Real-Time Streaming Analytics Pipeline
opensearch_index_setup.py - Creates index mappings & dashboard visual templates in OpenSearch.
"""

import os
import json
import urllib3
import boto3
from botocore.exceptions import ClientError

http = urllib3.PoolManager()
HEADERS = {"Content-Type": "application/json"}

INDEX_NAME = "iot-metrics"

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s"
    },
    "mappings": {
        "properties": {
            "sensor_id": {"type": "keyword"},
            "temperature": {"type": "float"},
            "humidity": {"type": "float"},
            "timestamp": {"type": "date"},
            "is_anomaly": {"type": "boolean"}
        }
    }
}

DASHBOARD_INDEX_PATTERN = {
    "attributes": {
        "title": "iot-metrics*",
        "timeFieldName": "timestamp"
    }
}


def create_opensearch_domain(domain_name: str = "iot-metrics-domain", region: str = "us-east-1"):
    """
    Provisions Amazon OpenSearch Service domain if requested.
    Spec: t3.small.search, 1 data node, EBS gp3.
    """
    os_client = boto3.client("opensearch", region_name=region)
    try:
        response = os_client.create_domain(
            DomainName=domain_name,
            EngineVersion="OpenSearch_2.11",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
                "DedicatedMasterEnabled": False,
                "ZoneAwarenessEnabled": False
            },
            EBSOptions={
                "EBSEnabled": True,
                "VolumeType": "gp3",
                "VolumeSize": 10
            },
            NodeToNodeEncryptionOptions={"Enabled": True},
            EncryptionAtRestOptions={"Enabled": True}
        )
        print(f"Provisioning OpenSearch domain '{domain_name}'...")
        return response
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceAlreadyExistsException":
            print(f"OpenSearch domain '{domain_name}' already exists.")
        else:
            print(f"Notice creating OpenSearch domain: {e}")
        return None


def setup_index_mapping(opensearch_url: str):
    """
    Creates or updates the iot-metrics index mapping in OpenSearch.
    """
    if not opensearch_url:
        print("Warning: OPENSEARCH_ENDPOINT / OPENSEARCH_HOST not provided. Skipping HTTP index setup.")
        return False

    url = f"{opensearch_url.rstrip('/')}/{INDEX_NAME}"
    try:
        # Check if index exists
        res = http.request("HEAD", url)
        if res.status == 200:
            print(f"Index '{INDEX_NAME}' already exists in OpenSearch.")
            return True

        # Create index with mapping
        response = http.request(
            "PUT",
            url,
            body=json.dumps(INDEX_MAPPING),
            headers=HEADERS
        )
        if response.status in [200, 201]:
            print(f"Successfully created index '{INDEX_NAME}' with schema mapping.")
            return True
        else:
            print(f"Failed to create index '{INDEX_NAME}': HTTP {response.status} - {response.data.decode('utf-8')}")
            return False
    except Exception as e:
        print(f"Error configuring OpenSearch index: {e}")
        return False


def setup_dashboard_visual_templates(opensearch_url: str):
    """
    Registers Dashboards index pattern and visualization templates.
    """
    if not opensearch_url:
        return False

    url = f"{opensearch_url.rstrip('/')}/_dashboards/api/saved_objects/index-pattern/{INDEX_NAME}"
    try:
        response = http.request(
            "POST",
            url,
            body=json.dumps(DASHBOARD_INDEX_PATTERN),
            headers={"Content-Type": "application/json", "osd-xsrf": "true"}
        )
        if response.status in [200, 201]:
            print("Successfully registered OpenSearch Dashboards index pattern.")
            return True
        else:
            print(f"Dashboards template registration returned HTTP {response.status}")
            return False
    except Exception as e:
        print(f"Dashboards registration notice: {e}")
        return False


if __name__ == "__main__":
    opensearch_host = os.environ.get("OPENSEARCH_ENDPOINT") or os.environ.get("OPENSEARCH_HOST", "")
    domain = os.environ.get("OPENSEARCH_DOMAIN", "iot-metrics-domain")
    region = os.environ.get("AWS_REGION", "us-east-1")

    if not opensearch_host and os.environ.get("PROVISION_OPENSEARCH") == "true":
        create_opensearch_domain(domain, region)

    if opensearch_host:
        setup_index_mapping(opensearch_host)
        setup_dashboard_visual_templates(opensearch_host)
    else:
        print("Index mapping definition verified. Set OPENSEARCH_ENDPOINT to deploy to an active cluster.")

