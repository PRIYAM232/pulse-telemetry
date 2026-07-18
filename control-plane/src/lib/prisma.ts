// Prisma Client singleton.
//
// Next.js dev mode hot-reloads modules on every edit; without this guard each
// reload would construct a fresh PrismaClient (and a fresh pg connection
// pool), exhausting PostgreSQL's connection limit within minutes. We stash
// the client on globalThis, which survives hot reloads, in non-production
// only — in production each server process should own exactly one client.
//
// Prisma 7 has no bundled Rust engine: the client talks to PostgreSQL through
// an explicit driver adapter (@prisma/adapter-pg → node-postgres pool).

import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "@/generated/prisma/client";

const globalForPrisma = globalThis as unknown as { prisma?: PrismaClient };

function createPrismaClient(): PrismaClient {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error(
      "DATABASE_URL is not set — the control plane cannot reach PostgreSQL. " +
        "Copy control-plane/.env or export it in the environment.",
    );
  }
  return new PrismaClient({ adapter: new PrismaPg({ connectionString }) });
}

export const prisma = globalForPrisma.prisma ?? createPrismaClient();

if (process.env.NODE_ENV !== "production") {
  globalForPrisma.prisma = prisma;
}
