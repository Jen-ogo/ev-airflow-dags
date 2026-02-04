from datetime import datetime, timedelta
import json
import random
import requests
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email

from azure.eventhub import EventHubProducerClient, EventData
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential

# ============================================================
# CONFIG
# ============================================================

DAG_ID = "traffic_tomtom_ingestion"

ADLS_CONTAINER = "uc-root"
TRAFFIC_SNAPSHOT_PREFIX = "bronze/traffic/candidates_snapshot/"
EMAIL_TO = ["coach.tutor1993@gmail.com"]

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 12, 22),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["traffic", "tomtom", "eventhub", "streaming"],
) as dag:

    # ============================================================
    # 1. LOAD CANDIDATE POINTS (DELTA-SAFE)
    # ============================================================
    @task
    def load_points():
        credential = ClientSecretCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"],
        )

        service = BlobServiceClient(
            f"https://{os.environ['AZURE_STORAGE_ACCOUNT']}.blob.core.windows.net",
            credential=credential,
        )

        container = service.get_container_client(ADLS_CONTAINER)

        # only real data files (ignore Delta metadata)
        blobs = [
            b for b in container.list_blobs(name_starts_with=TRAFFIC_SNAPSHOT_PREFIX)
            if b.name.endswith(".json")
            and "/_committed_" not in b.name
            and "/_started_" not in b.name
            and "/_SUCCESS" not in b.name
        ]

        if not blobs:
            raise RuntimeError("❌ No valid traffic candidate snapshot data files found")

        blob = sorted(blobs, key=lambda b: b.last_modified, reverse=True)[0]
        raw = container.download_blob(blob.name).readall().decode("utf-8")

        points = []
        for line in raw.splitlines():
            if not line.strip():
                continue

            r = json.loads(line)

            candidate_id = r.get("candidate_id") or r.get("id")
            if candidate_id is None:
                # silently skip garbage / non-data rows
                continue

            points.append({
                "candidate_id": candidate_id,
                "lat": r["lat"],
                "lon": r["lon"],
                "region": r["region"],
            })

        if not points:
            raise RuntimeError("❌ Snapshot parsed but no valid candidate points found")

        return points

    # ============================================================
    # 2. FEATURE FLAG
    # ============================================================
    @task
    def traffic_enabled(points):
        return points if Variable.get("TRAFFIC_ENABLED", "false") == "true" else []

    # ============================================================
    # 3. FETCH TRAFFIC (RAW PAYLOAD)
    # ============================================================
    @task
    def fetch_traffic(points):
        if not points:
            return {"events": [], "api_calls": 0}

        dry_run = Variable.get("TOMTOM_DRY_RUN", "true") == "true"
        api_key = Variable.get("TOMTOM_API_KEY")

        events = []
        api_calls = 0

        for p in points:
            if dry_run:
                payload = {
                    "mock": True,
                    "currentSpeed": random.randint(20, 90),
                    "confidence": round(random.uniform(0.7, 0.95), 2),
                }
            else:
                url = (
                    "https://api.tomtom.com/traffic/services/4/"
                    "flowSegmentData/absolute/10/json"
                    f"?point={p['lat']},{p['lon']}&key={api_key}"
                )
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                payload = r.json()
                api_calls += 1

            events.append({
                "candidate_id": p["candidate_id"],
                "region": p["region"],
                "ts": datetime.utcnow().isoformat(),
                "source": "TOMTOM",
                "dry_run": dry_run,
                "lat": p["lat"],                 # query point
                "lon": p["lon"],                 # query point
                "payload": json.dumps(payload),  # RAW JSON STRING
            })

        return {
            "events": events,
            "api_calls": api_calls,
        }

    # ============================================================
    # 4. PUSH TO EVENT HUB
    # ============================================================
    @task
    def push_to_eventhub(result):
        if Variable.get("EVENTHUB_ENABLED", "false") != "true":
            return {"sent": 0, "api_calls": result["api_calls"]}

        events = result["events"]
        if not events:
            return {"sent": 0, "api_calls": result["api_calls"]}

        producer = EventHubProducerClient.from_connection_string(
            Variable.get("EVENTHUB_CONN_STR"),
            eventhub_name=Variable.get("EVENTHUB_NAME"),
        )

        sent = 0
        with producer:
            batch = producer.create_batch()
            for e in events:
                batch.add(EventData(json.dumps(e)))
                sent += 1
            producer.send_batch(batch)

        return {
            "sent": sent,
            "api_calls": result["api_calls"],
        }

    # ============================================================
    # 5. SUCCESS EMAIL
    # ============================================================
    @task
    def send_success_email(stats):
        send_email(
            to=EMAIL_TO,
            subject="[SUCCESS] Traffic TomTom ingestion",
            html_content=f"""
            <h3>✅ Traffic ingestion completed</h3>
            <p><b>DAG:</b> {DAG_ID}</p>
            <p><b>Run time (UTC):</b> {datetime.utcnow().isoformat()}</p>
            <hr/>
            <p><b>TomTom API calls:</b> {stats["api_calls"]}</p>
            <p><b>Events sent to Event Hub:</b> {stats["sent"]}</p>
            """,
        )

    # ============================================================
    # PIPELINE
    # ============================================================
    points = load_points()
    enabled = traffic_enabled(points)
    traffic = fetch_traffic(enabled)
    stats = push_to_eventhub(traffic)
    send_success_email(stats)
