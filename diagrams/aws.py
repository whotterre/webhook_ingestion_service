from diagrams import Diagram, Cluster, Edge
from diagrams.aws.network import CloudFront, ALB
from diagrams.aws.compute import Fargate, ElasticContainerServiceTask
from diagrams.aws.database import Aurora, ElastiCache
from diagrams.onprem.queue import Kafka

with Diagram("WebHook Ingestion Service (AWS)", show=False, direction="LR", graph_attr = {
    "dpi": "200",
    "bgcolor": "white",
    "fontname": "Helvetica",
    "fontsize": "14",
    "pad": "0.5",
    "nodesep": "0.8",
    "ranksep": "1.2",
    "splines": "spline",   # curved edges instead of right angles
}):
    with Cluster("VPC"):
        cdn = CloudFront("DDoS Protection")
        lb = ALB("Load Balancer")

        with Cluster("Ingest Tier"):
            ingest = Fargate("Ingest Service")

        buffer = Kafka("Event Log Buffer")

        with Cluster("Worker Tier"):
            workers = ElasticContainerServiceTask("Worker Pool")

        cache = ElastiCache("Idempotency Cache")
        ledger = Aurora("ACID Ledger")

        # Linear flow
        cdn >> lb >> ingest >> buffer >> workers

        # Fan-out from workers (parallel, not chained)
        workers >> Edge(label="dedup check") >> cache
        workers >> Edge(label="commit") >> ledger