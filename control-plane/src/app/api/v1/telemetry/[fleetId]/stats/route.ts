// POST /api/v1/telemetry/:fleetId/stats
//
// Heartbeat sink for otelcol-pulse data planes. Every stats interval each
// collector reports how many spans and log records it received and dropped
// since its last successful report; one FleetMetric row is inserted per
// report. Auth is the same fleet-scoped bearer token as the policy pull
// (src/lib/fleet-auth.ts).
//
// Request body (produced by the stats loop in
// data-plane/processors/filterprocessor/engine.go):
//
//   {
//     "traces_received":  int64, "traces_dropped":  int64,
//     "logs_received":    int64, "logs_dropped":    int64,
//     "metrics_received": int64, "metrics_dropped": int64
//   }
//
// Phase-4 collectors still in the field send { "received", "dropped" }; those
// are accepted and recorded as trace counts so a fleet can upgrade its
// control plane before its collectors. Pre-Phase-6 collectors simply omit the
// metrics_* keys, which read as 0 below.
//
// An all-zero body is still a valid heartbeat — it proves the collector is
// alive — so it is stored, not skipped.

import { NextRequest, NextResponse } from "next/server";
import { authenticateFleet } from "@/lib/fleet-auth";
import { prisma } from "@/lib/prisma";

export const dynamic = "force-dynamic";

// FleetMetric counters are Postgres INT4. The collector accumulates across
// failed reports, so after a very long control-plane outage on a very hot
// fleet the delta could exceed int32; clamp instead of rejecting so one
// oversized report can't wedge the heartbeat into a permanent 400 loop.
const INT4_MAX = 2_147_483_647;

// A count field may be absent (older collector, or a signal the pipeline
// doesn't carry) — that reads as 0. Present-but-malformed is a client bug and
// stays a 400.
function asCount(value: unknown): number | null {
  if (value === undefined) return 0;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    return null;
  }
  return Math.min(value, INT4_MAX);
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ fleetId: string }> },
): Promise<NextResponse> {
  const { fleetId } = await params;

  const auth = await authenticateFleet(request, fleetId);
  if (!auth.ok) {
    return auth.response;
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "request body must be JSON" },
      { status: 400 },
    );
  }

  const payload = (body ?? {}) as Record<string, unknown>;
  const tracesReceived = asCount(payload.traces_received ?? payload.received);
  const tracesDropped = asCount(payload.traces_dropped ?? payload.dropped);
  const logsReceived = asCount(payload.logs_received);
  const logsDropped = asCount(payload.logs_dropped);
  const metricsReceived = asCount(payload.metrics_received);
  const metricsDropped = asCount(payload.metrics_dropped);
  if (
    tracesReceived === null ||
    tracesDropped === null ||
    logsReceived === null ||
    logsDropped === null ||
    metricsReceived === null ||
    metricsDropped === null
  ) {
    return NextResponse.json(
      { error: "telemetry counts must be non-negative integers" },
      { status: 400 },
    );
  }

  await prisma.fleetMetric.create({
    data: {
      fleetId: auth.fleetId,
      tracesReceived,
      tracesDropped,
      logsReceived,
      logsDropped,
      metricsReceived,
      metricsDropped,
    },
  });

  return new NextResponse(null, { status: 204 });
}
