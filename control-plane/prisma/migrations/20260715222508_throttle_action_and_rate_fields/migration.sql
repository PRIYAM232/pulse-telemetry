-- AlterEnum
ALTER TYPE "PolicyAction" ADD VALUE 'THROTTLE';

-- AlterTable
ALTER TABLE "PolicyRule" ADD COLUMN     "throttleGroupBy" TEXT,
ADD COLUMN     "throttleRate" INTEGER;
