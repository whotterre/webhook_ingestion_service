from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka, RabbitMQ
from diagrams.onprem.network import Nginx
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.monitoring import Prometheus, Grafana
from diagrams.onprem.client import Users
from diagrams.generic.storage import Storage
from diagrams.aws.storage import S3
from diagrams.generic.compute import Rack
from diagrams.generic.network import Firewall
from diagrams.programming.language import Go

# Graph attributes for a clean look
graph_attr = {
    "fontsize": "20",
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "spline",
    "rankdir": "LR" # Left to Right flow
}

with Diagram("Webhook Ingestion & Financial Services", show=False, graph_attr=graph_attr, filename="webhook_ingestion_arch"):
    
    # --- External Entities ---
    psp = Users("PSP Simulator\n(Webhooks)")
    psp_api = Server("PSP REST API\n(Polling)")
    client = Users("Internal User\nService")

    # --- Edge & Ingest Tier ---
    with Cluster("Edge & Ingest Tier"):
        proxy = Nginx("Reverse Proxy\n(TLS)")
        with Cluster("Ingest Workers"):
            ingest = [Go("ingest-worker") for _ in range(3)]

    # --- Message Brokers & Dispatch ---
    with Cluster("Message Brokers & Dispatch"):
        kafka = Kafka("Kafka\nraw.events")
        dispatcher = Go("Dispatcher\n(Kafka -> RMQ)")
        rmq = RabbitMQ("RabbitMQ\nExchange + 64 Queues")
        
    # --- Storage & Replay ---
    with Cluster("Archive & Recovery"):
        obj_store = S3("Object Store\nRaw Archive")
        dlq = Storage("DLQ")
        replay = Go("Replay CLI")

    # --- NEW: Financial Services ---
    with Cluster("New Financial Services"):
        with Cluster("Account Generation"):
            acc_svc = Go("Account Number\nService")
            lock = Redis("Redis\nDistributed Lock")
        
        with Cluster("Payment Status"):
            pay_status_svc = Go("Payment Status\nService")
            circuit_breaker = Firewall("Circuit Breaker\n& Poller")

    # --- Worker Tier ---
    with Cluster("Worker Tier"):
        pay_verifier = Go("payment-verifier\n(Webhook Fallback)")
        ledger_worker = Go("ledger-worker\n(Saga Coordinator)")
        notifier = Go("notifier-worker")
        analytics = Go("analytics-worker")

    # --- State & Observability ---
    with Cluster("State Stores"):
        dedup_db = Redis("Redis\nBloom Filter Dedup")
        postgres = PostgreSQL("PostgreSQL\nLedger + Outbox")

    with Cluster("Observability"):
        prom = Prometheus("Prometheus")
        grafana = Grafana("Grafana")

    # ==========================================
    #                  FLOWS
    # ==========================================

    # 1. Ingestion Flow
    psp >> Edge(label="HTTP POST") >> proxy
    proxy >> Edge(label="Route") >> ingest[0]
    proxy >> Edge(label="Route") >> ingest[1]
    proxy >> Edge(label="Route") >> ingest[2]
    
    for worker in ingest:
        worker >> Edge(label="Produce") >> kafka

    # 2. Dispatch & Archive Flow
    kafka >> Edge(label="Consume") >> dispatcher
    dispatcher >> Edge(label="Archive", style="dashed") >> obj_store
    dispatcher >> Edge(label="Publish") >> rmq

    # 3. DLQ & Replay
    rmq >> Edge(label="Poison", color="red", style="dashed") >> dlq
    obj_store >> Edge(label="Replay", style="dotted") >> replay
    replay >> Edge(label="Re-inject", style="dotted") >> rmq

    # 4. Worker Tier Flow
    rmq >> Edge(label="Route") >> pay_verifier
    rmq >> Edge(label="Route") >> ledger_worker
    rmq >> Edge(label="Route") >> notifier
    rmq >> Edge(label="Route") >> analytics

    # 5. Payment Status Flow (The new polling logic)
    pay_verifier >> Edge(label="Request Status") >> pay_status_svc
    pay_status_svc >> Edge(label="Query") >> circuit_breaker
    circuit_breaker >> Edge(label="API Call", color="blue") >> psp_api
    pay_verifier >> Edge(label="Check", style="dotted") >> dedup_db
    pay_verifier >> Edge(label="Event Confirmed", style="bold") >> rmq

    # 6. Account Generation Flow (User Onboarding)
    client >> Edge(label="Create Account") >> acc_svc
    acc_svc >> Edge(label="Acquire Lock", style="dotted") >> lock
    acc_svc >> Edge(label="Save Account + Outbox") >> postgres

    # 7. Ledger & Saga Flow
    ledger_worker >> Edge(label="Check", style="dotted") >> dedup_db
    ledger_worker >> Edge(label="Write State") >> postgres
    ledger_worker >> Edge(label="Emit Saga Event") >> rmq

    # 8. Metrics (Dotted lines to keep it clean)
    prom >> Edge(style="dotted", color="gray") >> grafana
    kafka >> Edge(style="dotted", color="gray") >> prom
    rmq >> Edge(style="dotted", color="gray") >> prom
    postgres >> Edge(style="dotted", color="gray") >> prom