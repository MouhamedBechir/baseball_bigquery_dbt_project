#!/bin/bash
# =============================================================
# share_access.sh
# Grants Viewer access to reviewers on GCP project
#
# Usage:
#   chmod +x share_access.sh
#   ./share_access.sh YOUR_PROJECT_ID reviewer@email.com
# =============================================================

set -e

PROJECT_ID=${1:-"baseballdataplatform"}
REVIEWER_EMAIL=${2:-"email"}

echo "Granting BigQuery viewer access to: $REVIEWER_EMAIL"

# BigQuery data viewer
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$REVIEWER_EMAIL" \
  --role="roles/bigquery.dataViewer"

# BigQuery job user (needed to run queries in Looker Studio)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$REVIEWER_EMAIL" \
  --role="roles/bigquery.jobUser"

echo "Access granted successfully!"
echo ""
