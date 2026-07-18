My local `ARCHITECTURE.md` or system documentation file.
Here is the complete, production-ready tech stack description for **Pulse**. 

---

# Pulse Architecture & Tech Stack

Pulse utilizes a **"Split-Brain" Architecture** to maximize both real-time data-processing efficiency and developer iteration speed. The system is strictly partitioned into a compiled, ultra-low-latency **Data Plane** and a web-accessible, schema-driven **Control Plane**.

---

## 1. The Data Plane (Telemetry Engine)

The Data Plane sits inline with your application's telemetry stream. It must parse, evaluate, and act on millions of telemetry data events per second without adding network hops or meaningful CPU overhead.

* **Language:** **Go (Golang)**
* *Why:* Memory-safe, statically compiled, highly concurrent execution environment natively compatible with the core OpenTelemetry ecosystem.


* **Compilation Toolchain:** **OpenTelemetry Collector Builder (`ocb`)**
* *Why:* The official CNCF command-line utility used to generate custom OpenTelemetry Collector distributions. It reads a YAML manifest (`builder-config.yaml`) and compiles a single, lightweight binary containing only the necessary telemetry inputs alongside your custom inline filtering rules.


* **Core Packages:** * `go.opentelemetry.io/collector/component` (Component lifecycle management)
* `go.opentelemetry.io/collector/processor` (Custom telemetry interception mechanics)
* `sync.Mutex` (Safe in-memory state swaps when rules change dynamically)



---

## 2. The Control Plane (Fleet Management & API)

The Control Plane is the operational brain. It handles user authentication, exposes the management interface, models filtering logic, and serves configurations down to the distributed Data Plane collectors via a poll-and-sync API.

* **Framework:** **Next.js (React) & Node.js (TypeScript)**
* *Why:* Provides a highly scannable, unified environment for developing both the visual dashboard and the downstream config APIs. Node's asynchronous event-driven I/O allows the sync server to effortlessly handle high-frequency polling requests from thousands of active OTel sidecars simultaneously.


* **Database ORM:** **Prisma**
* *Why:* Provides a strongly typed abstraction layer over the database, allowing for rapid schema updates, automated relational mapping, and deterministic state management for fleet configurations.


* **Styling Layer:** **Tailwind CSS + Radix UI / Shadcn**
* *Why:* Clean, low-weight component assembly to fast-track visual dashboards tracking ingestion cost trends, active dropping rates, and system latency.



---

## 3. Storage Layer

* **Database:** **PostgreSQL**
* *Why:* Enterprise-grade relational stability. Essential for storing policy mappings (`PolicyRule`), fleet identification states (`CollectorFleet`), relational user scopes, and time-stamped telemetry savings metrics for historical cost calculations.



---

## Structural Component Topology

```
[ Microservices / AI Agents ]
             │ 
             │ (High-throughput OTLP Streams)
             ▼
┌────────────────────────────────────────────────────────┐
│  DATA PLANE: Pulse Custom OTel Collector Binary (Go)   │
│  - Reads in-memory JSON rules                          │
│  - Drops, redacts, or mutates data in-memory (<1ms)     │
└──────────────────────────▲─────────────────────────────┘
                           │
                           │ (Periodic Polling via REST over HTTP/gRPC)
                           │
┌──────────────────────────┴─────────────────────────────┐
│  CONTROL PLANE: Management Platform (TypeScript)        │
│  ┌───────────────────────┐   ┌──────────────────────┐  │
│  │  Next.js UI Frontend  │   │  Node.js Config API  │  │
│  └───────────────────────┘   └──────────┬───────────┘  │
└─────────────────────────────────────────┼──────────────┘
                                          │
                                          ▼ (Strongly-Typed ORM via Prisma)
                                ┌───────────────────┐
                                │    PostgreSQL     │
                                └───────────────────┘

```

---

## Tech Stack Summary Matrix

| Architectural Layer | Language | Core Tech Stack | Primary Performance Constraint |
| --- | --- | --- | --- |
| **Data Plane Proxy** | Go | `ocb` + OTel Core SDK | **Sub-millisecond Latency** (<1ms processing overhead, zero network hops) |
| **Control Plane API** | TypeScript | Node.js + Next.js App Router | **High I/O Concurrency** (Efficiently serving JSON payloads to polling clients) |
| **Control Plane UI** | TypeScript | React + Tailwind CSS | **Time-to-Value Rendering** (Instant data visualization and rule toggles) |
| **Persistence Engine** | SQL | PostgreSQL + Prisma | **Relational Integrity** (Predictable schemas for multi-tenant fleet policies) |