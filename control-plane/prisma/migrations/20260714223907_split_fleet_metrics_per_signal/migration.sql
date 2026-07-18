-- Phase 5: FleetMetric goes multi-signal. Hand-written (not prisma-diffed)
-- because the diff would DROP received/dropped and lose every heartbeat row
-- collected so far; RENAME keeps the history — pre-Phase-5 counts were
-- span counts, so they land in the traces columns. The new log columns
-- default to 0 so old rows read as "no log traffic".

ALTER TABLE "FleetMetric" RENAME COLUMN "received" TO "tracesReceived";
ALTER TABLE "FleetMetric" RENAME COLUMN "dropped" TO "tracesDropped";

ALTER TABLE "FleetMetric"
  ADD COLUMN "logsReceived" INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN "logsDropped" INTEGER NOT NULL DEFAULT 0;
