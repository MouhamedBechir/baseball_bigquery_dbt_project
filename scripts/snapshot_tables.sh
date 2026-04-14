#!/bin/bash
# =============================================================
# snapshot_tables.sh
# Creates point-in-time BigQuery table snapshots for backup
#
# Usage:
#   chmod +x snapshot_tables.sh
#   ./snapshot_tables.sh YOUR_PROJECT_ID
# =============================================================

set -e

PROJECT_ID=${1:-"redsox-data-platform"}
DATE=$(date +%Y%m%d)
EXPIRY_DAYS=7   # snapshots expire after 7 days (free tier)

TABLES=(
  "baseball_mart.fct_games"
  "baseball_mart.fct_postseason_games"
  "baseball_mart.agg_team_season_stats"
  "baseball_mart.agg_redsox_performance"
  "baseball_mart.agg_pitcher_stats"
  "baseball_mart.dim_teams"
  "baseball_mart.dim_dates"
)

echo "=================================================="
echo " Creating BigQuery table snapshots — $DATE"
echo "=================================================="

for TABLE in "${TABLES[@]}"; do
  DATASET=$(echo $TABLE | cut -d. -f1)
  TABLE_NAME=$(echo $TABLE | cut -d. -f2)
  SNAPSHOT_NAME="${TABLE_NAME}_snapshot_${DATE}"

  echo "Snapshotting: $TABLE → ${DATASET}.${SNAPSHOT_NAME}"

  bq cp \
    --snapshot \
    --expiration=$(( EXPIRY_DAYS * 86400 )) \
    "${PROJECT_ID}:${TABLE}" \
    "${PROJECT_ID}:${DATASET}.${SNAPSHOT_NAME}"
done

echo ""
echo "All snapshots created successfully!"
echo "They will expire in $EXPIRY_DAYS days."
