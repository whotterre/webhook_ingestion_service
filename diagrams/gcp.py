from diagrams import Diagram, Cluster, Edge
from diagrams.gcp.network import LoadBalancing
from diagrams.gcp.compute import Run, GKE
from diagrams.gcp.database import SQL, Memorystore
from diagrams.gcp.analytics import PubSub

with Diagram("WebHook Ingestion Service (GCP)",
           show=False, direction="LR", graph_attr = {
    "dpi": "200",
    "bgcolor": "white",
    "fontname": "Helvetica",
    "fontsize": "14",
    "pad": "0.5",
    "nodesep": "0.8",
    "ranksep": "1.2",
    "splines": "spline",
}):
    armor = LoadBalancing("Cloud Armor + Global LB")

    with Cluster("VPC"):
        with Cluster("Ingest Tier"):
            ingest = Run("Cloud Run\nIngest")

        buffer = PubSub("Pub/Sub\nraw.events")

        with Cluster("Worker Tier"):
            unordered = Run("Cloud Run\nNotifier + Analytics")
            ordered = GKE("GKE\nLedger Consumer\n(ordering keys)")

        cache = Memorystore("Memorystore\nIdempotency")
        ledger = SQL("Cloud SQL /\nAlloyDB")

    armor >> ingest >> buffer
    buffer >> Edge(label="push") >> unordered
    buffer >> Edge(label="pull + ordering key") >> ordered
    unordered >> cache
    ordered >> cache
    ordered >> ledger