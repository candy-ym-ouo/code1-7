-- CreateEnum
CREATE TYPE "MediaTaskStatus" AS ENUM ('PENDING', 'QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED');

-- AlterTable
ALTER TABLE "Recording" ADD COLUMN "uploadKey" TEXT;

-- CreateTable
CREATE TABLE "MediaTask" (
    "recordingId" TEXT NOT NULL,
    "status" "MediaTaskStatus" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lastError" TEXT,
    "lockedAt" TIMESTAMP(3),
    "lockedBy" TEXT,
    "startedAt" TIMESTAMP(3),
    "finishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MediaTask_pkey" PRIMARY KEY ("recordingId")
);

-- CreateIndex
CREATE UNIQUE INDEX "Recording_uploadKey_key" ON "Recording"("uploadKey");

-- CreateIndex
CREATE INDEX "MediaTask_status_updatedAt_idx" ON "MediaTask"("status", "updatedAt");

-- AddForeignKey
ALTER TABLE "MediaTask" ADD CONSTRAINT "MediaTask_recordingId_fkey" FOREIGN KEY ("recordingId") REFERENCES "Recording"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Backfill：为存量录音补建发件箱任务，避免升级后老录音停在 PROCESSING
INSERT INTO "MediaTask" ("recordingId", "status", "attempts", "updatedAt")
SELECT
    r."id",
    CASE
        WHEN r."status" = 'READY' THEN 'COMPLETED'::"MediaTaskStatus"
        WHEN r."status" = 'FAILED' THEN 'FAILED'::"MediaTaskStatus"
        ELSE 'PENDING'::"MediaTaskStatus"
    END,
    CASE WHEN r."status" = 'PROCESSING' THEN 1 ELSE 0 END,
    CURRENT_TIMESTAMP
FROM "Recording" r
WHERE NOT EXISTS (
    SELECT 1 FROM "MediaTask" t WHERE t."recordingId" = r."id"
);
