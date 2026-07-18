-- AlterEnum
ALTER TYPE "ConditionOp" ADD VALUE 'REGEX_MATCH';

-- AlterTable
ALTER TABLE "FleetMetric" ADD COLUMN     "metricsDropped" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "metricsReceived" INTEGER NOT NULL DEFAULT 0;
