// Fleet-scoped bearer authentication, shared by every /api/v1 route the data
// plane calls (policy pull, telemetry stats push).
//
// Security notes:
//   - Key comparison is constant-time (timingSafeEqual) to avoid leaking key
//     prefixes through response timing.
//   - Unknown fleet and wrong key both return the same 401 so the endpoints
//     cannot be used to enumerate fleet IDs.

import { timingSafeEqual } from "node:crypto";
import { NextRequest, NextResponse } from "next/server";
import { prisma } from "@/lib/prisma";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function constantTimeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a, "utf8");
  const bufB = Buffer.from(b, "utf8");
  // timingSafeEqual throws on length mismatch; a length check leaks only the
  // key length, which is not secret.
  return bufA.length === bufB.length && timingSafeEqual(bufA, bufB);
}

function unauthorized(): NextResponse {
  return NextResponse.json(
    { error: "invalid fleet credentials" },
    { status: 401, headers: { "WWW-Authenticate": "Bearer" } },
  );
}

export type FleetAuthResult =
  | { ok: true; fleetId: string }
  | { ok: false; response: NextResponse };

/**
 * Validates the fleetId path segment and the `Authorization: Bearer` header
 * against CollectorFleet.apiKey. On failure, `response` is ready to return
 * as-is (400 for a malformed fleetId, 401 for anything credential-shaped).
 */
export async function authenticateFleet(
  request: NextRequest,
  fleetId: string,
): Promise<FleetAuthResult> {
  if (!UUID_RE.test(fleetId)) {
    return {
      ok: false,
      response: NextResponse.json(
        { error: "fleetId must be a UUID" },
        { status: 400 },
      ),
    };
  }

  const authorization = request.headers.get("authorization");
  const bearer = authorization?.match(/^Bearer\s+(\S+)$/i)?.[1];
  if (!bearer) {
    return { ok: false, response: unauthorized() };
  }

  const fleet = await prisma.collectorFleet.findUnique({
    where: { id: fleetId },
    select: { id: true, apiKey: true },
  });
  if (!fleet || !constantTimeEqual(bearer, fleet.apiKey)) {
    return { ok: false, response: unauthorized() };
  }

  return { ok: true, fleetId: fleet.id };
}
