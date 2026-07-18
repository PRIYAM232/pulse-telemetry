# Pulse Control Plane

Fleet management API for the `otelcol-pulse` data plane: Next.js 16 (App
Router, TypeScript), Prisma 7, PostgreSQL.

Collectors poll `GET /api/v1/policies/:fleetId` with their fleet's API key and
receive the fleet's dynamic `PolicyRule` set (`DROP` / `SAMPLE` / `REDACT`
against traces, logs, or metrics). The Go side of this contract lives in
`data-plane/processors/filterprocessor/config.go` — keep the two in sync.

## Layout

| Path | Purpose |
| --- | --- |
| `prisma/schema.prisma` | `CollectorFleet` + `PolicyRule` models and enums |
| `prisma/seed.ts` | Deterministic dev fleet + 3 sample rules (idempotent upserts) |
| `src/lib/prisma.ts` | Prisma Client singleton (hot-reload safe, pg driver adapter) |
| `src/app/api/v1/policies/[fleetId]/route.ts` | Bearer-authenticated policy endpoint |

## Local development

```bash
# 1. PostgreSQL (from the repo root; data persists in the pulse-pgdata volume)
docker compose -f deploy/docker/docker-compose.yaml up -d postgres

# 2. Migrate + seed (from control-plane/)
npm install
npx prisma migrate dev
npx prisma db seed

# 3. API server on :3000
npm run dev
```

The seed provisions fleet `f1ee7000-0000-4000-8000-000000000001` with API key
`pulse_dev_sk_2f7d1b9c4e8a4f60b3d5a9c1e6f80712` (**local dev only — rotate for
any real deployment**). Smoke-test:

```bash
curl -s http://localhost:3000/api/v1/policies/f1ee7000-0000-4000-8000-000000000001 \
  -H "Authorization: Bearer pulse_dev_sk_2f7d1b9c4e8a4f60b3d5a9c1e6f80712" | jq
```

Wrong or missing key → generic `401` (fleet IDs are not enumerable); malformed
fleet ID → `400`. Key comparison is constant-time.

## End-to-end with the data plane

With postgres + the dev server up, build and run the collector (repo root):

```bash
make build && make run
```

`data-plane/config/otelcol-dev.yaml` points `pulse_filter` at this API with a
10s sync interval. Send test spans:

```bash
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@v0.156.0
telemetrygen traces --otlp-insecure --otlp-endpoint localhost:4317 \
  --traces 2 --telemetry-attributes 'http.status_code="404"'   # dropped
telemetrygen traces --otlp-insecure --otlp-endpoint localhost:4318 --otlp-http \
  --traces 2 --telemetry-attributes 'http.status_code="200"'   # passes
```

Rule edits (SQL, Prisma Studio, or a future dashboard) propagate to running
collectors within one sync interval — no restart.

## Roadmap

- ETag/`If-None-Match` on the policy endpoint so unchanged polls are cheap 304s.
- Dashboard: ingestion cost trends, drop rates, fleet health.
- CRUD API + UI for fleets and rules (Phase 3).
