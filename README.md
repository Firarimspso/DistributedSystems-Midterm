# Distributed Systems Midterm

## Environment setup

This project uses Python 3 in a local virtual environment and requires Git,
Docker Desktop, and a Kubernetes cluster with at least one Ready node.

```bash
source .venv/bin/activate
python --version
docker version
kubectl get nodes
```

Runtime data is kept under `data/`. The virtual environment, secrets, Python
cache files, and runtime storage data are excluded from Git.

## Section 2: storage nodes

Each FastAPI storage node stores chunks by their lowercase SHA-256 hash. PUT
validates the request body against the URL hash, while GET and HEAD recompute
the on-disk hash and return `409 Conflict` if corruption is detected. DELETE
removes a chunk, and `/health` reports the node ID. Each node also attempts to
POST a heartbeat every five seconds to `METADATA_URL`; an unavailable metadata
service is logged without interrupting storage operations.

Install the development dependencies and run the automated tests:

```bash
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
pytest -q
```

Build and start all four independent nodes:

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
curl http://localhost:8004/health
```

The host ports are 8001 through 8004, and the corresponding persistent data
directories are `data/node1` through `data/node4`. To start just one node, use
`docker compose up --build -d node1` (or another service name).

Example chunk lifecycle against node1:

```bash
CONTENT='example chunk'
HASH=$(printf '%s' "$CONTENT" | shasum -a 256 | awk '{print $1}')
curl -X PUT --data-binary "$CONTENT" "http://localhost:8001/chunks/$HASH"
curl "http://localhost:8001/chunks/$HASH"
curl -I "http://localhost:8001/chunks/$HASH"
curl -X DELETE "http://localhost:8001/chunks/$HASH"
```

## Section 3: metadata and placement

The metadata service runs on `localhost:9000` and coordinates four storage
nodes through a deterministic SHA-256 consistent-hashing ring with 128 virtual
nodes per physical node. PostgreSQL is exposed on host port `5434` (container
port 5432) because port 5433 is already used by the macOS development
environment. Metadata is stored in the `storage_metadata` database using the
`storage` role; Docker Compose supplies the development password.

The service persists bucket, object key, monotonically increasing version,
original size, UTC creation time, and ordered chunk hashes. Separate chunk and
placement tables hold chunk size, reference count, and physical node. Database
constraints prevent duplicate versions and negative reference counts.

### Chunking strategies

- `fixed` (default) uses ordered 4 MiB chunks.
- `cdc` uses a real FastCDC-style 64-bit gear rolling hash with configurable
  minimum, average, and maximum sizes (defaults: 1, 4, and 8 MiB).

For the deterministic shifted-data test (80,000 original bytes plus a
518-byte prefix), 1 KiB fixed chunking produced 79/79 chunks and zero shared
chunk hashes. CDC with 256/1024/4096-byte bounds produced 55/56 chunks and 54
shared hashes, demonstrating boundary resynchronization and better
deduplication after insertion.

### Deduplication and deletion

An upload physically stores each unique SHA-256 chunk on its ring-selected
primary node before committing metadata. Identical chunks share one chunk row;
reference counts include repeated occurrences within one object version.
Deleting a version decrements counts and sends physical DELETE only for chunks
whose count becomes zero. The ring can also return multiple distinct nodes for
future replication, but Section 3 stores one primary copy and implements no
Section 4 redundancy or erasure coding.

Storage nodes POST heartbeats every five seconds to `/heartbeats`. The metadata
service records node ID, free bytes, chunk count, and last heartbeat; `/nodes`
marks a node inactive after 15 seconds without an update.

### Run and test Section 3

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:9000/health
curl http://localhost:9000/nodes

source .venv/bin/activate
python -m pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql+psycopg://storage:storage@localhost:5434/storage_metadata_test \
  pytest -q -s
```

Example metadata upload, lookup, and version deletion:

```bash
curl -X POST --data-binary @README.md \
  'http://localhost:9000/objects/course/readme?strategy=fixed'
curl 'http://localhost:9000/objects/course/readme?version=1'
curl -X DELETE 'http://localhost:9000/objects/course/readme?version=1'
```

Placement tests use the same 1,000 deterministic SHA-256 chunk hashes for both
comparisons. With 128 virtual nodes per physical node, the measured primary
movement is **18.60% when adding node5** and **24.20% when removing node4**.

### Consistency limitations

PostgreSQL row locks, transactions, uniqueness constraints, and reference-count
checks protect metadata. Physical storage and PostgreSQL cannot share one
atomic transaction: failed uploads attempt to remove chunks written during the
request, while failed physical deletes roll back metadata. A process crash
between those systems can leave an orphan chunk or, during a final delete, a
metadata reference whose file was already removed. Concurrent first uploads of
the same new chunk or concurrent creation of the same object version can also
race; the unique constraints reject inconsistent metadata, but a production
system should add idempotency keys, per-object advisory locks, and a background
reconciliation/garbage-collection job.

## Section 4: redundancy and recovery

The default bucket policy is three-way replication with write quorum `W=2`
and read quorum `R=2`. Policy validation enforces `W <= N`, `R <= N`, and
`W + R > N`. Replicas are placed on distinct physical nodes selected by the
consistent-hashing ring. A write is acknowledged only after W successful node
writes, and `/data/{bucket}/{key}` returns data only after R distinct replicas
pass SHA-256 verification.

Six storage nodes now run on ports 8001–8006. This makes real Reed–Solomon 4+2
placement possible with one shard per independent container and persistent
directory. zfec generates four data and two parity shards; any four valid
shards reconstruct the original chunk, which is then checked against its
content SHA-256. Three missing/corrupt shards correctly make 4+2 unreadable.

Configure replication or erasure coding per bucket:

```bash
curl -X PUT http://localhost:9000/buckets/critical/redundancy \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"replication","replication_factor":3,"read_quorum":2,"write_quorum":2,"data_shards":4,"parity_shards":2}'

curl -X PUT http://localhost:9000/buckets/archive/redundancy \
  -H 'Content-Type: application/json' \
  -d '{"strategy":"erasure","replication_factor":3,"read_quorum":2,"write_quorum":2,"data_shards":4,"parity_shards":2}'
```

### Failure repair and scrubbing

Nodes become inactive after 15 seconds without a heartbeat. The repair loop
runs every 10 seconds: replicated chunks are copied from a verified replica to
another healthy distinct node, and erasure shards are reconstructed when at
least k valid shards exist and enough distinct healthy nodes are available.
The scrubber runs every 30 seconds, rereads replicas/shards, verifies SHA-256,
and replaces detected corrupt data. Results and transferred bytes are persisted
in `repair_events` and exposed at `/repairs`.

Manual verification endpoints are also available:

```bash
curl -X POST http://localhost:9000/repairs/run
curl -X POST http://localhost:9000/scrub/run
curl http://localhost:9000/repairs
curl http://localhost:9000/nodes
```

### Calculated overhead comparison

Run `python -m metadata_service.comparison` for the reproducible table below.
Physical bytes are calculated from actual zfec shard lengths. Repair traffic
models one lost replica/shard and includes bytes read plus bytes written.

| Workload | Strategy | Logical bytes | Physical bytes | Overhead | Repair traffic |
|---|---:|---:|---:|---:|---:|
| 1,000 × 4 KiB objects | replication 3x | 4,096,000 | 12,288,000 | 3.000x | 8,192,000 |
| 1,000 × 4 KiB objects | erasure 4+2 | 4,096,000 | 6,144,000 | 1.500x | 5,120,000 |
| 8 × 4 MiB objects | replication 3x | 33,554,432 | 100,663,296 | 3.000x | 67,108,864 |
| 8 × 4 MiB objects | erasure 4+2 | 33,554,432 | 50,331,648 | 1.500x | 41,943,040 |

Run all tests and the stack:

```bash
docker compose up --build -d
TEST_DATABASE_URL=postgresql+psycopg://storage:storage@localhost:5434/storage_metadata_test \
  pytest -q -s
docker compose ps
```

Recovery is application-level rather than a distributed transaction. A crash
between PostgreSQL and node operations can require reconciliation. Repair can
restore the requested protection only when enough verified source data and
enough distinct active destination nodes exist. The worker is intentionally
single-service and uses database event records rather than a distributed job
queue; replication across metadata-service instances is outside Section 4.

### Final workload benchmark artifacts

`benchmarks/section4_workloads.py` deterministically generates the professor's
two required workload size lists: 5,000 objects from 1–50 KiB and 10 objects
from 100 MiB–1 GiB. It calculates full-workload redundancy costs without
creating gigabytes of payload data and separately times a bounded actual zfec
sample. Exact baseline output is preserved in
`benchmarks/results/section4-workloads.md`. The write, read, and failure/repair
flowcharts for the report are in `docs/section4-flowcharts.md`.

```bash
python -m benchmarks.section4_workloads
pytest -q tests/test_section4_benchmark.py
```

## Section 5: authenticated object gateway

The public FastAPI gateway is exposed on `localhost:8080` and delegates object
storage, metadata, redundancy, and cluster state to the existing metadata
service. Supply `X-API-Key: dev-secret-key` in the development configuration.
API keys, the per-key request limit, and its time window are configured with
`API_KEYS`, `RATE_LIMIT_REQUESTS`, and `RATE_LIMIT_WINDOW_SECONDS`.

| Method | Endpoint | Purpose |
|---|---|---|
| POST/GET | `/buckets` | Create or list buckets |
| PUT | `/buckets/{bucket}/objects/{key}` | Stream an object upload |
| GET | `/buckets/{bucket}/objects/{key}` | Download; supports one HTTP byte range |
| DELETE | `/buckets/{bucket}/objects/{key}` | Delete latest or requested version |
| GET | `/buckets/{bucket}/objects/{key}/metadata` | Object metadata and tags |
| GET | `/buckets/{bucket}/objects` | List/search by prefix, size, dates, and JSON tags |
| GET | `/cluster/status` | Nodes, free bytes, chunk counts, degraded objects |

Example:

```bash
API_KEY=dev-secret-key
curl -H "X-API-Key: $API_KEY" -X POST http://localhost:8080/buckets \
  -H 'Content-Type: application/json' -d '{"name":"course-data"}'
curl -H "X-API-Key: $API_KEY" -H 'X-Object-Tags: {"type":"notes"}' \
  -X PUT --data-binary @README.md \
  http://localhost:8080/buckets/course-data/objects/readme.md
curl -H "X-API-Key: $API_KEY" -H 'Range: bytes=0-99' \
  http://localhost:8080/buckets/course-data/objects/readme.md
```

Run gateway tests locally without rebuilding containers:

```bash
pytest -q tests/test_gateway.py
```

For a PostgreSQL volume created before Section 5, apply the additive,
data-preserving migration once before starting the updated metadata service:

```bash
docker compose exec -T postgres psql -U storage -d storage_metadata \
  -f /dev/stdin < docker/section5-migration.sql
```

## Section 6: React operations console and Elasticsearch

The gateway serves a lightweight React 18 interface at
`http://localhost:8080/ui/`. It requires no npm build step: the static source
uses React's production UMD browser bundle and calls the gateway on the same
origin. Enter an API key to browse/create buckets, stream uploads, list or
search objects, download, delete, and inspect cluster health. The cluster panel
shows each node's status, free space, chunk count, degraded-object count, and
recent repair/scrub events.

Elasticsearch runs as an optional single-node Compose service on port 9200.
Successful gateway uploads index their returned metadata; deletions remove the
corresponding version document. `/search/objects` supports bucket, key prefix,
size, creation date, and JSON tag filters. Storage remains authoritative in
PostgreSQL and the storage nodes: an Elasticsearch indexing outage is logged
but never makes a successful object upload fail.

```bash
# Once the updated images are available:
docker compose up -d elasticsearch gateway1 gateway2 nginx
open http://localhost:8080/ui/

# Local tests require neither service:
pytest -q tests/test_gateway.py
```

The React CDN files require browser network access on first load. For a fully
offline final demonstration, vendor the pinned React 18 UMD files under `web/`
and update `web/index.html`. Live Elasticsearch and browser testing were not
performed during the deadline-constrained implementation; the REST integration,
Compose configuration, static UI serving, and injected search behavior are
covered locally by pytest.

## Section 7: load balancing and fault tolerance

Nginx is the public entry point on port 8080 and uses least-connections load
balancing across `gateway1` and `gateway2`. Both gateway processes use the same
metadata, Elasticsearch, and storage services. `config/nginx.conf` disables
buffering so streamed uploads are forwarded without Nginx staging the body.

RabbitMQ exposes AMQP on host port 5673 and its management console on 15673,
avoiding standard ports used by unrelated local services. The durable
`maintenance` queue accepts `repair`, `rebalance`, and `garbage_collection`
jobs. `maintenance-worker` consumes jobs and calls idempotent metadata
maintenance endpoints. Failed work is republished with the same job ID and a
bounded incremented attempt count.

```bash
python -m workers.jobs enqueue repair
python -m workers.jobs enqueue rebalance
python -m workers.jobs enqueue garbage_collection
```

Prometheus is configured on host port 9091 and Grafana on 3001. Prometheus
scrapes both gateways, metadata, and all six storage nodes. The provisioned
dashboard covers request throughput, latency, disk space, degraded chunks, and
repair rate. Metrics include:

- `gateway_requests_total` and `gateway_request_latency_seconds_sum`
- `storage_node_disk_free_bytes` and `storage_node_chunks`
- `storage_degraded_chunks` and `storage_repairs_total`

`k8s/storage-statefulset.yaml` defines six stable storage pods, per-pod 10 GiB
PVCs, and readiness/liveness probes. Its image name is a placeholder that must
be built and published before deployment.

`k8s/etcd-ha-template.yaml` is explicitly a **deployment template only** for a
three-member persistent etcd cluster. The working metadata service still uses
one PostgreSQL container; it is not wired to etcd, and PostgreSQL streaming
replication is not configured. Metadata high availability must not be claimed
as live or complete.

Validate configuration without starting services:

```bash
docker compose config --quiet
python -c 'import yaml; [list(yaml.safe_load_all(open(p))) for p in ["k8s/storage-statefulset.yaml","k8s/etcd-ha-template.yaml"]]'
pytest -q tests/test_jobs.py tests/test_section7_config.py
```

Limitations: Section 7 containers were not started live; RabbitMQ management
API consumption removes a message before executing it, so a worker crash in
that narrow interval can lose a job even though ordinary failures are retried;
rate-limit counters remain local to each gateway; and etcd is not an active
metadata backend.

## Section 8: packaging, EKS, autoscaling, and scale-out

Each deployable component now has a separate image definition:

| Service | Dockerfile |
|---|---|
| Storage node | `Dockerfile.storage` |
| Metadata service | `Dockerfile.metadata` |
| API gateway | `Dockerfile.gateway` |
| Maintenance worker | `Dockerfile.worker` |
| React/Nginx UI | `Dockerfile.ui` |

Compose uses these Dockerfiles directly. Nginx remains the local public entry
point on port 8080 and routes `/ui/` to the UI container and API traffic across
the two gateway containers.

```bash
# Local deployment (builds only when explicitly requested):
docker compose config --quiet
docker compose up --build -d
docker compose ps
```

### AWS EKS deployment template

The `k8s/aws/` directory contains namespace/configuration, gateway, metadata,
storage, worker, UI, and HPA manifests. Replace every
`REPLACE_WITH_...` value first. The templates expect:

- an existing EKS cluster with Metrics Server and the AWS EBS CSI driver;
- five images published in Amazon ECR;
- an externally reachable PostgreSQL database, normally Amazon RDS;
- RabbitMQ and Elasticsearch endpoints reachable from the cluster;
- appropriate IAM, security groups, DNS, TLS, and secrets.

Suggested deployment sequence (not executed by this project):

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
# Build/tag/push each service-specific Dockerfile only after replacing ACCOUNT.

kubectl apply -f k8s/aws/config.yaml
kubectl apply -f k8s/aws/storage.yaml
kubectl apply -f k8s/aws/metadata.yaml
kubectl apply -f k8s/aws/worker.yaml
kubectl apply -f k8s/aws/gateway.yaml
kubectl apply -f k8s/aws/gateway-hpa.yaml
kubectl apply -f k8s/aws/ui.yaml
kubectl -n distributed-storage get deploy,statefulset,pods,svc,hpa,pvc
```

The gateway HPA uses `autoscaling/v2`, targets 70% average CPU utilization,
keeps at least two gateway pods, and scales to at most five. CPU requests are
defined so utilization-based scaling has a valid denominator.

### Reproducible scale-out demonstration

The lightweight demonstration creates 1,000 tiny SHA-256-addressed chunks on a
four-node consistent-hash ring, adds node5, moves only chunks whose target
changes, and verifies every hash after movement. It uses the production ring
and the same content-addressed copy/delete behavior as the metadata rebalance
endpoint. No files or persistent volumes are touched.

```bash
python -m benchmarks.scale_out_demo
```

Actual local result on the submission machine:

```text
Chunks:  1000
Moved:   186 (18.60%)
Traffic: 1838 bytes
SHA-256: PASS
```

The detailed result is preserved in
`benchmarks/results/section8-scale-out.md`. `/maintenance/rebalance` now moves
replicated chunks to the current active-node ring and records rebalance events.

### Section 8 limitations

AWS/EKS was not deployed, no ECR images were pushed, and no paid cloud resource
was created. Docker images were not built during implementation. Kubernetes
validation is syntactic/local only. Live persistent-volume rebalancing was not
executed; the recorded result is an actual in-memory scale-out measurement.
The EKS template depends on external PostgreSQL, RabbitMQ, and Elasticsearch
services and requires production TLS, IAM, backup, and secret management before
real use.
