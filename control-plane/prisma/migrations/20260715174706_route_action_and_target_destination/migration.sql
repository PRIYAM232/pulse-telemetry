-- AlterEnum
ALTER TYPE "PolicyAction" ADD VALUE 'ROUTE';

-- AlterTable
ALTER TABLE "PolicyRule" ADD COLUMN     "targetDestination" TEXT;
