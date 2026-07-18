// GET /api/v1/policies/:fleetId
//
// The polling endpoint for otelcol-pulse data planes. Auth is a fleet-scoped
// bearer token: `Authorization: Bearer <CollectorFleet.apiKey>` — see
// src/lib/fleet-auth.ts for the comparison/enumeration guarantees.
//
// Conditional polling: every response carries a strong ETag (SHA-256 of the
// exact body bytes). Collectors echo it back as If-None-Match; on a match we
// return 304 with an empty body so an idle fleet costs headers, not payloads.
//
// Response shape (mirrored by FleetPolicyResponse in
// data-plane/processors/filterprocessor/config.go):
//
//   { "fleet_id": "<uuid>", "rules": [ PolicyRule, ... ] }

import { createHash } from "node:crypto";
import { NextRequest, NextResponse } from "next/server";
import { authenticateFleet } from "@/lib/fleet-auth";
import { prisma } from "@/lib/prisma";

// Auth headers make this dynamic anyway; declare it so a future refactor
// can't accidentally opt the route into static caching.
export const dynamic = "force-dynamic";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ fleetId: string }> },
): Promise<NextResponse> {
  const { fleetId } = await params;

  const auth = await authenticateFleet(request, fleetId);
  if (!auth.ok) {
    return auth.response;
  }

  const rules = await prisma.policyRule.findMany({
    where: { fleetId: auth.fleetId },
    orderBy: { createdAt: "asc" },
  });

  // Serialize once and hash those exact bytes. The ETag, the 304 comparison,
  // and the body the data plane SHA-256s for change detection are then all
  // views of the same string — no risk of a re-serialization differing.
  const body = JSON.stringify({ fleet_id: auth.fleetId, rules });
  const etag = `"${createHash("sha256").update(body).digest("hex")}"`;

  // `no-cache` (not `no-store`): intermediaries may hold the response but
  // must revalidate — which is exactly the ETag contract; `private` keeps it
  // out of shared caches since the body is fleet-scoped.
  const sharedHeaders = {
    ETag: etag,
    "Cache-Control": "private, no-cache",
  };

  // Exact comparison is sufficient: the only clients are our collectors,
  // which echo the ETag verbatim (never weakened to W/ or joined in a list).
  if (request.headers.get("if-none-match") === etag) {
    return new NextResponse(null, { status: 304, headers: sharedHeaders });
  }

  return new NextResponse(body, {
    status: 200,
    headers: { ...sharedHeaders, "Content-Type": "application/json" },
  });
}
