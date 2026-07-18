# Pulse on Kubernetes — `pulse-telemetry` Helm chart

Phase 9: the docker-compose stack (`deploy/docker`) as a Helm release.
Phase 10 adds self-monitoring (collector metrics on :8888, ServiceMonitor,
Grafana dashboard — see [Self-monitoring](#self-monitoring-phase-10)).

```
deploy/helm/pulse-telemetry/
├── Chart.yaml
├── values.yaml
├── files/
│   └── pulse-collector-dashboard.json  # Grafana dashboard (inlined by .Files.Get)
└── templates/
    ├── _helpers.tpl              # labels, DATABASE_URL + control-plane URL helpers
    ├── secret.yaml               # DATABASE_URL for the control plane
    ├── configmap-collector.yaml  # otelcol-k8s.yaml pipeline config
    ├── postgres.yaml             # StatefulSet + PVC template + headless Service
    ├── migration-job.yaml        # prisma migrate deploy + db seed (Helm hook)
    ├── control-plane.yaml        # Next.js Deployment + Service :3000
    ├── collector.yaml            # otelcol-pulse Deployment + Service :4317/:4318/:8888
    ├── servicemonitor.yaml       # Prometheus Operator scrape config (gated)
    ├── dashboard-configmap.yaml  # Grafana sidecar dashboard (gated)
    └── NOTES.txt
```

Startup order, mirroring compose: postgres (ready) → migrate+seed (hook Job)
→ control plane → collector. On **first install** the migration Job runs as a
`post-install` hook (a `pre-install` hook would fire before the in-chart
postgres exists); on **upgrades** it runs `pre-upgrade`, so migrations always
complete before new app pods roll out. Resource names derive from the release
name — install as `pulse` to get the `pulse-control-plane` / `pulse-collector`
/ `pulse-postgres` DNS names the collector config expects.

## 1. Build the images

From the repo root (same builds the compose stack uses):

```sh
docker build -t pulse/control-plane:dev         --target runner  control-plane/
docker build -t pulse/control-plane-migrate:dev --target builder control-plane/
docker build -t pulse/otelcol-pulse:dev -f deploy/docker/Dockerfile.collector .
```

## 2. Load them into a local cluster

kind:

```sh
kind create cluster --name pulse
kind load docker-image --name pulse \
  pulse/control-plane:dev pulse/control-plane-migrate:dev pulse/otelcol-pulse:dev
```

minikube:

```sh
minikube start
minikube image load pulse/control-plane:dev
minikube image load pulse/control-plane-migrate:dev
minikube image load pulse/otelcol-pulse:dev
```

## 3. Install

```sh
helm lint deploy/helm/pulse-telemetry
helm install pulse deploy/helm/pulse-telemetry --namespace pulse --create-namespace
kubectl -n pulse get pods -w
```

Expected steady state: `pulse-postgres-0`, `pulse-control-plane-*`, and
`pulse-collector-*` Running/Ready; `pulse-migrate-*` Completed.

## 4. Test

```sh
# migrations + seeded dev fleet
kubectl -n pulse logs job/pulse-migrate

# dashboard
kubectl -n pulse port-forward svc/pulse-control-plane 3000:3000
open http://localhost:3000/dashboard

# OTLP smoke test through the collector
kubectl -n pulse port-forward svc/pulse-collector 4318:4318
curl -sS -X POST http://localhost:4318/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"helm-smoke-test"}}]},"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"smoke","kind":1,"startTimeUnixNano":"1750000000000000000","endTimeUnixNano":"1750000001000000000"}]}]}]}'

# pulse_filter batch observations + debug exporter output
kubectl -n pulse logs deploy/pulse-collector -f
```

Scale the data plane:

```sh
helm upgrade pulse deploy/helm/pulse-telemetry -n pulse --reuse-values --set collector.replicas=3
```

## Self-monitoring (Phase 10)

Every collector pod serves its own metrics — Prometheus text format on
`:8888/metrics`, exposed as the `metrics` port on the pod and the
`pulse-collector` Service. Alongside the stock `otelcol_*` process/pipeline
metrics, the `pulse_filter` engine registers:

| Metric | Type | Meaning |
| --- | --- | --- |
| `otelcol_pulse_filter_rules` | gauge | rules in the currently synced ruleset (0 until first sync) |
| `otelcol_pulse_filter_spans_received` / `_dropped` | counter | spans in / removed by rules |
| `otelcol_pulse_filter_logs_received` / `_dropped` | counter | log records in / removed |
| `otelcol_pulse_filter_metrics_received` / `_dropped` | counter | metrics in / removed |

Counters are cumulative since process start (Prometheus `rate()` material) —
unlike the control-plane heartbeat, which reports interval deltas. The
chart's telemetry reader pins classic collector names (`without_type_suffix`
etc.), so there is no `_total` suffix.

Discovery is opt-in, off by default:

```sh
helm upgrade pulse deploy/helm/pulse-telemetry -n pulse --reuse-values \
  --set metrics.serviceMonitor.enabled=true \
  --set 'metrics.serviceMonitor.additionalLabels.release=kube-prometheus-stack' \
  --set metrics.dashboard.enabled=true
```

- `metrics.serviceMonitor.enabled` — a `monitoring.coreos.com/v1`
  ServiceMonitor scraping every collector pod on the `metrics` port.
  Requires the Prometheus Operator CRDs; match `additionalLabels` to your
  Prometheus instance's `serviceMonitorSelector` (kube-prometheus-stack
  defaults to `release: <its-release-name>`).
- `metrics.dashboard.enabled` — a ConfigMap labelled `grafana_dashboard: "1"`
  carrying `files/pulse-collector-dashboard.json`; Grafana's dashboard
  sidecar imports it automatically. Use `metrics.dashboard.namespace` if the
  sidecar only watches its own namespace, and the `grafana_folder`
  annotation for folder routing.

### Simulating a Prometheus scrape against kind

After rebuilding + `kind load`-ing the collector image (the self-metrics are
compiled into the binary) and upgrading the release:

```sh
# 1. Exactly what Prometheus does — hit one pod's :8888 from inside the
#    cluster (ServiceMonitor scrapes pod endpoints, never the ClusterIP):
POD_IP=$(kubectl -n pulse get pod -l app.kubernetes.io/name=pulse-collector \
  -o jsonpath='{.items[0].status.podIP}')
kubectl -n pulse run scrape-sim --rm -it --restart=Never \
  --image=curlimages/curl -- curl -s "http://${POD_IP}:8888/metrics"

# 2. Or from the workstation:
kubectl -n pulse port-forward deploy/pulse-collector 8888:8888
curl -s localhost:8888/metrics | grep pulse_filter

# 3. Push a span through OTLP (see the smoke test above), re-scrape, and
#    watch otelcol_pulse_filter_spans_received climb. With the seeded dev
#    fleet's rules enabled in the dashboard, otelcol_pulse_filter_rules
#    goes non-zero within one sync interval (10s).
```

Every pod behind the Service is scraped individually; `kubectl -n pulse get
endpoints pulse-collector` lists the addresses Prometheus will target.

## Real environments

- **Managed database**: `--set postgres.enabled=false --set database.externalUrl=postgresql://…` —
  drops the StatefulSet entirely; secret, migration job, and control plane
  all follow the external URL.
- **Fleet credentials**: defaults are the deterministic dev seed values —
  rotate `fleet.id` / `fleet.key` (and set `migrator.seed=false`) for any
  real deployment.
- **External producers**: `--set collector.service.type=LoadBalancer` to
  expose OTLP 4317/4318 outside the cluster.

Teardown: `helm uninstall pulse -n pulse` (PVC survives; `kubectl -n pulse
delete pvc data-pulse-postgres-0` to drop the data), then
`kind delete cluster --name pulse`.
