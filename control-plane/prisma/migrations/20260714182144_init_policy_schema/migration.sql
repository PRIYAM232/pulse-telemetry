-- CreateEnum
CREATE TYPE "PolicyAction" AS ENUM ('DROP', 'SAMPLE', 'REDACT');

-- CreateEnum
CREATE TYPE "TargetSignal" AS ENUM ('TRACES', 'LOGS', 'METRICS');

-- CreateEnum
CREATE TYPE "ConditionOp" AS ENUM ('EQUALS', 'GREATER_THAN', 'CONTAINS', 'EXISTS');

-- CreateTable
CREATE TABLE "CollectorFleet" (
    "id" UUID NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "apiKey" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CollectorFleet_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PolicyRule" (
    "id" UUID NOT NULL,
    "fleetId" UUID NOT NULL,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "name" TEXT NOT NULL,
    "actionType" "PolicyAction" NOT NULL,
    "sampleRate" DOUBLE PRECISION,
    "targetSignal" "TargetSignal" NOT NULL,
    "conditionField" TEXT NOT NULL,
    "conditionOp" "ConditionOp" NOT NULL,
    "conditionValue" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PolicyRule_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "CollectorFleet_apiKey_key" ON "CollectorFleet"("apiKey");

-- CreateIndex
CREATE INDEX "PolicyRule_fleetId_isActive_idx" ON "PolicyRule"("fleetId", "isActive");

-- AddForeignKey
ALTER TABLE "PolicyRule" ADD CONSTRAINT "PolicyRule_fleetId_fkey" FOREIGN KEY ("fleetId") REFERENCES "CollectorFleet"("id") ON DELETE CASCADE ON UPDATE CASCADE;
