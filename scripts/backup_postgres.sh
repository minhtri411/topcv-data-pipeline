#!/bin/bash
set -e

BACKUP_DIR="/backup"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"

echo "[INFO] Starting PostgreSQL backup..."

pg_dump \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  --format=custom \
  -f "${BACKUP_DIR}/topcv_${DATE}.dump"

echo "[INFO] Backup created: ${BACKUP_DIR}/topcv_${DATE}.dump"

# Keep backups from the last 30 days
find "$BACKUP_DIR" \
  -name "topcv_*.dump" \
  -mtime +30 \
  -delete

echo "[INFO] PostgreSQL backup completed."