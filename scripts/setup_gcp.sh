#!/bin/bash

set -e

PROJECT_ID=${1:-"project_id"}
REGION="US"

echo "=================================================="
echo " RedSox Data Platform — GCP Setup"
echo " Project: $PROJECT_ID"
echo "=================================================="

# ── 1. Auth ───────────────────────────────────────────
echo ""
echo "[1/5] Authenticating with Google Cloud..."
gcloud auth login
gcloud auth application-default login

# ── 2. Create project ─────────────────────────────────
echo ""
echo "[2/5] Creating GCP project: $PROJECT_ID"
gcloud projects create $PROJECT_ID --name="baseballdataplatform" || echo "Project may already exist, continuing..."
gcloud config set project $PROJECT_ID

# ── 3. Enable APIs ────────────────────────────────────
echo ""
echo "[3/5] Enabling required APIs..."
gcloud services enable bigquery.googleapis.com
gcloud services enable bigquerydatatransfer.googleapis.com

# ── 4. Create BigQuery datasets ───────────────────────
echo ""
echo "[4/5] Creating BigQuery datasets..."

bq mk --dataset \
  --location=$REGION \
  --description="Raw landing layer — 1:1 copy of source tables" \
  $PROJECT_ID:baseball_raw

bq mk --dataset \
  --location=$REGION \
  --description="Staging layer — cleaned and standardised" \
  $PROJECT_ID:baseball_staging

bq mk --dataset \
  --location=$REGION \
  --description="Mart layer — business-ready aggregated tables" \
  $PROJECT_ID:baseball_mart

bq mk --dataset \
  --location=$REGION \
  --description="Dev environment — safe for testing" \
  $PROJECT_ID:baseball_dev

echo ""
echo "[5/5] Datasets created:"
bq ls --project_id=$PROJECT_ID

echo ""
echo "=================================================="
echo " Setup complete!"
echo ""
echo " Next steps:"
echo "   1. Copy profiles.yml to ~/.dbt/profiles.yml"
echo "   2. Replace YOUR_GCP_PROJECT_ID with: $PROJECT_ID"
echo "   3. Run: dbt debug"
echo "   4. Run: dbt deps"
echo "   5. Run: dbt build"
echo "=================================================="
