# Multi-stage build for the Pulse control plane (Next.js + Prisma).
# Build context is this directory:
#   docker build -t pulse/control-plane:dev control-plane/
#
# Two consumable stages:
#   builder — full node_modules + Prisma CLI + schema/migrations/seed. The
#             docker-compose stack runs this stage as a ONE-SHOT migrator
#             (`prisma migrate deploy && prisma db seed`) before the app
#             starts, so the runtime image never needs the Prisma CLI at all.
#   runner  — the standalone `next build` output only: server.js plus the
#             dependency subset Next's file tracing proved the app imports
#             (the generated Prisma client and pg driver ride along there).

FROM node:24-alpine AS builder
WORKDIR /app

# Take Alpine security fixes the base image hasn't been rebuilt with yet
# (e.g. OpenSSL 3.5.8-r0 for CVE-2026-14456 while node:24-alpine still ships
# 3.5.7-r0). BuildKit caches this layer per base-image digest, so later
# fixes land when node:24-alpine is republished or the build cache is cleared.
RUN apk upgrade --no-cache

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# The generated client lives in src/generated/prisma (gitignored convention;
# .dockerignore doesn't exclude it, but regenerate regardless so the image
# never depends on the host's generator having run).
RUN npx prisma generate

# `next build` imports route modules while collecting page data, and
# src/lib/prisma.ts throws at import time without DATABASE_URL. The pg pool
# is lazy — nothing connects during build — so any syntactically valid URL
# satisfies it. The real URL arrives from the environment at runtime.
ENV DATABASE_URL=postgresql://build:build@localhost:5432/build \
    NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:24-alpine AS runner
WORKDIR /app

# Same Alpine fixes as the builder, and no npm: the runtime only ever runs
# `node server.js`, while npm's bundled tar, brace-expansion and ip-address
# carry HIGH advisories (still unpatched in npm 12.0.2). The migrator
# (builder stage) keeps npm because migrations run through `npx prisma`.
RUN apk upgrade --no-cache \
 && rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx

ENV NODE_ENV=production \
    PORT=3000 \
    HOSTNAME=0.0.0.0 \
    NEXT_TELEMETRY_DISABLED=1

USER node

# Standalone output expects exactly this layout: server.js and its traced
# node_modules at the root, static assets and public/ alongside (the minimal
# server serves them itself when present — no CDN in this stack).
COPY --from=builder --chown=node:node /app/.next/standalone ./
COPY --from=builder --chown=node:node /app/.next/static ./.next/static
COPY --from=builder --chown=node:node /app/public ./public

EXPOSE 3000

CMD ["node", "server.js"]
