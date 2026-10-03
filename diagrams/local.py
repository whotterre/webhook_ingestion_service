from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.network import Nginx, HAProxy
from diagrams.onprem.compute import Server
from diagrams.onprem.queue import Kafka, RabbitMQ
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.monitoring import Prometheus, Grafana
from diagrams.aws.storage import S3
from diagrams.onprem.client import Users

graph_attr = {
    "dpi": "200",
    "bgcolor": "white",
    "fontname": "Helvetica",
    "fontsize": "14",
    "pad": "0.5",
    "nodesep": "0.8",
    "ranksep": "1.2",
    "splines": "spline",
}

with Diagram("Webhook Ingestion Service (Local)", show=False,
             direction="LR", graph_attr=graph_attr):

    psp = Users("PSP Simulator")

    with Cluster("Edge"):
        proxy = HAProxy("Reverse Proxy\n(TLS termination)")

    with Cluster("Ingest Tier"):
        ingest = [
            Server("ingest-1"),
            Server("ingest-2"),
            Server("ingest-3"),
        ]

    buffer = Kafka("Kafka\nraw.events\n(retention: 30d)")

    with Cluster("Dispatch"):
        dispatcher = Server("Dispatcher\n(Kafka -> RMQ)")

    with Cluster("Broker"):
        rmq = RabbitMQ("RabbitMQ\nmodulus-hash exchange\n64 queues + SAC")

    with Cluster("Worker Tier"):
        workers = [
            Server("ledger-worker"),
            Server("notifier-worker"),
            Server("analytics-worker"),
        ]

    with Cluster("State"):
        cache = Redis("Redis\nBloom (dedup)")
        ledger = PostgreSQL("Postgres\nledger + idempotency")

    with Cluster("Replay / Recovery"):
        dlq = RabbitMQ("DLQ")
        archive = S3("Object Store\nraw archive")
        replay = Server("Replay CLI")

    with Cluster("Observability"):
        prom = Prometheus("Prometheus")
        graf = Grafana("Grafana")

    # Ingest path
    psp >> proxy >> ingest
    for i in ingest:
        i >> buffer

    # Dispatch
    buffer >> dispatcher >> rmq

    # Workers
    rmq >> workers

    # Worker effects (fan-out)
    for w in workers:
        w >> Edge(label="dedup") >> cache
    workers[0] >> Edge(label="commit") >> ledger  # ledger worker only

    # Failure path
    workers >> Edge(label="poison", style="dashed", color="red") >> dlq
    dlq >> Edge(label="replay", style="dashed", color="orange") >> replay
    archive >> Edge(label="rebuild", style="dashed", color="orange") >> replay
    replay >> Edge(style="dashed", color="orange") >> rmq
    buffer >> Edge(label="archive", style="dotted") >> archive

    # Observability (dotted so it doesn't clutter the main flow)
    for i in ingest + workers:
        i >> Edge(style="dotted", color="gray") >> prom
    prom >> graf