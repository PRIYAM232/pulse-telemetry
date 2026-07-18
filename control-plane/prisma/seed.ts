// Seed script — provisions one deterministic dev fleet plus three policy
// rules so the data plane can be tested immediately after `prisma db seed`.
//
// Everything is upserted against FIXED UUIDs, so the script is idempotent and
// the IDs/API key below can be hardcoded in data-plane/config/otelcol-dev.yaml.
// These credentials are for LOCAL DEVELOPMENT ONLY.
//
// Run via the Prisma CLI (configured in prisma.config.ts):  npx prisma db seed

import "dotenv/config";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../src/generated/prisma/client";

export const DEV_FLEET_ID = "f1ee7000-0000-4000-8000-000000000001";
export const DEV_FLEET_API_KEY =
  "pulse_dev_sk_2f7d1b9c4e8a4f60b3d5a9c1e6f80712";

const RULE_DROP_404 = "0a000000-0000-4000-8000-000000000001";
const RULE_DROP_HEALTHZ = "0a000000-0000-4000-8000-000000000002";
const RULE_SAMPLE_CHECKOUT = "0a000000-0000-4000-8000-000000000003";

const prisma = new PrismaClient({
  adapter: new PrismaPg({ connectionString: process.env.DATABASE_URL! }),
});

async function main() {
  const fleet = await prisma.collectorFleet.upsert({
    where: { id: DEV_FLEET_ID },
    update: { apiKey: DEV_FLEET_API_KEY },
    create: {
      id: DEV_FLEET_ID,
      name: "local-dev-fleet",
      description:
        "Deterministic development fleet for local end-to-end testing of the pulse_filter sync loop.",
      apiKey: DEV_FLEET_API_KEY,
    },
  });

  const rules = [
    {
      id: RULE_DROP_404,
      name: "drop-404-spans",
      isActive: true,
      actionType: "DROP",
      sampleRate: null,
      targetSignal: "TRACES",
      conditionField: "http.status_code",
      conditionOp: "EQUALS",
      conditionValue: "404",
    },
    {
      id: RULE_DROP_HEALTHZ,
      name: "drop-healthcheck-noise",
      isActive: true,
      actionType: "DROP",
      sampleRate: null,
      targetSignal: "TRACES",
      conditionField: "http.route",
      conditionOp: "CONTAINS",
      conditionValue: "/healthz",
    },
    {
      // Exercises forward compatibility: the Phase 2 data plane only executes
      // DROP, so it must fetch this rule, hold it in cache, and skip it
      // without erroring until SAMPLE lands in Phase 3.
      id: RULE_SAMPLE_CHECKOUT,
      name: "sample-checkout-service",
      isActive: true,
      actionType: "SAMPLE",
      sampleRate: 0.25,
      targetSignal: "TRACES",
      conditionField: "service.name",
      conditionOp: "EQUALS",
      conditionValue: "checkout-service",
    },
  ] as const;

  for (const rule of rules) {
    const { id, ...fields } = rule;
    await prisma.policyRule.upsert({
      where: { id },
      update: { ...fields },
      create: { id, fleetId: fleet.id, ...fields },
    });
  }

  console.log(`Seeded fleet "${fleet.name}"`);
  console.log(`  fleet_id : ${fleet.id}`);
  console.log(`  api_key  : ${DEV_FLEET_API_KEY}`);
  console.log(`  rules    : ${rules.map((r) => r.name).join(", ")}`);
  console.log(
    `\nPoll it:\n  curl -s http://localhost:3000/api/v1/policies/${fleet.id} \\\n    -H "Authorization: Bearer ${DEV_FLEET_API_KEY}" | jq`,
  );
}

main()
  .catch((err) => {
    console.error("Seed failed:", err);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
