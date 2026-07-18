-- CreateTable
CREATE TABLE "FleetMetric" (
    "id" UUID NOT NULL,
    "fleetId" UUID NOT NULL,
    "received" INTEGER NOT NULL,
    "dropped" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FleetMetric_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "FleetMetric_fleetId_createdAt_idx" ON "FleetMetric"("fleetId", "createdAt");

-- AddForeignKey
ALTER TABLE "FleetMetric" ADD CONSTRAINT "FleetMetric_fleetId_fkey" FOREIGN KEY ("fleetId") REFERENCES "CollectorFleet"("id") ON DELETE CASCADE ON UPDATE CASCADE;
