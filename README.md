# Baseball Data Platform — dbt + BigQuery + Looker Studio

A scalable data platform for MLB team analysis,
built on Google BigQuery using dbt for transformation and Looker Studio for visualisation.

---

## Architecture

```
bigquery-public-data.baseball   ← Source (public dataset)
         │
         ▼
  ┌─────────────┐
  │  RAW Layer  │  baseball_raw.*        Full copies of source tables
  └──────┬──────┘
         │
         ▼
  ┌──────────────────┐
  │  STAGING Layer   │  baseball_staging.*   Cleaned, typed, renamed
  └──────────┬───────┘
             │
             ▼
  ┌──────────────────┐
  │   MART Layer     │  baseball_mart.*      Business-ready aggregates
  └──────────────────┘
         │
         ▼
  Looker Studio Dashboards
```

---

## Table Inventory (12 tables)

| Layer | Table | Description |
|-------|-------|-------------|
| Raw | `raw_games_wide` | Regular season games (partitioned by date) |
| Raw | `raw_schedules` | Game schedule data |
| Raw | `raw_games_post_wide` | Postseason games |
| Staging | `stg_games` | Cleaned game facts |
| Staging | `stg_teams` | Unique team list |
| Staging | `stg_schedules` | Cleaned schedules |
| Staging | `stg_postseason` | Cleaned postseason games |
| Mart | `dim_teams` | Team dimension (division, league, RedSox flag) |
| Mart | `dim_dates` | Date spine 2000–2030 |
| Mart | `fct_games` | Core fact table — one row per game (incremental) |
| Mart | `fct_postseason_games` | Postseason fact table |
| Mart | `agg_team_season_stats` | Wins/losses/runs per team per season |
| Mart | `agg_redsox_performance` | Red Sox-only performance summary |
| Mart | `agg_team_stats` | Team performance statistics |

---

## Prerequisites

- Python 3.8+
- Google Cloud SDK (`gcloud`)
- A GCP project with BigQuery API enabled

---

## Quick Start

### 1. Clone & install dependencies

```bash
git clone <your-repo-url>
cd baseball_dataplatform

python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install dbt-bigquery
dbt deps
```

### 2. Set up GCP

```bash
chmod +x scripts/setup_gcp.sh
./scripts/setup_gcp.sh YOUR_PROJECT_ID
```

### 3. Configure dbt profile

Copy `profiles.yml` to `~/.dbt/profiles.yml` and replace `YOUR_GCP_PROJECT_ID`:

```bash
cp profiles.yml ~/.dbt/profiles.yml
# Then edit ~/.dbt/profiles.yml and set your project ID
```

### 4. Test the connection

```bash
dbt debug
```

### 5. Run all models

```bash
# Full build (run + test)
dbt build

# Or step by step:
dbt run --select raw
dbt run --select staging
dbt run --select mart
dbt test
```

### 6. View documentation

```bash
dbt docs generate
dbt docs serve
# Opens at http://localhost:8080
```

---

## Running in Dev vs Prod

```bash
# Dev (safe, lands in baseball_dev dataset)
dbt run --target dev

# Prod (lands in baseball_mart dataset)
dbt run --target prod
```

---

## Batch Schedule

For automated daily runs, use **Cloud Scheduler** to trigger a Cloud Run job or
Cloud Function that runs `dbt build --target prod`.

Example cron: `0 6 * * *` (runs at 06:00 UTC daily)

---

## Looker Studio Dashboard Setup

### Connect data sources

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Create → Report → BigQuery connector
3. Add these tables from `baseball_mart`:
   - `agg_team_season_stats` — Team overview page
   - `agg_redsox_performance` — RedSox deep dive page
   - `fct_games` — Game explorer page
   - `dim_teams` — Team filters

### Dashboard pages

**Page 1 — Red Sox Overview**
- Scorecards: Total games, wins, win %, league rank
- Line chart: Performance over seasons
- Bar chart: Runs scored vs league average
- Filters: Season range

**Page 2 — Team Analysis**
- Table: All teams ranked by performance
- Scatter plot: Runs vs win rate
- League comparison metrics
- Filters: Division, season

**Page 3 — League Comparison**
- Head-to-head team comparisons
- Performance tiers and rankings
- League averages and standard deviations
- Filters: Season, team selection

**Page 4 — Game Analysis**
- Individual game results
- Team performance trends
- Postseason summaries

---

## Backup & Recovery

```bash
chmod +x scripts/snapshot_tables.sh
./scripts/snapshot_tables.sh YOUR_PROJECT_ID
```

Snapshots expire after 7 days. BigQuery also provides 7-day time travel
automatically — you can restore any table with:

```sql
SELECT * FROM `project.dataset.table`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
```

---

## Sharing Access for Review

```bash
chmod +x scripts/share_access.sh
./scripts/share_access.sh YOUR_PROJECT_ID reviewer@email.com
```

---

## Assumptions

1. The public BigQuery dataset `bigquery-public-data.baseball` is treated as the source system — no separate ingestion pipeline needed.
2. Raw layer is a **full snapshot** (not streaming). Staging is views. Mart facts are incremental.
3. Batch frequency is **daily** for production use.
4. Column names (`homeTeamName`, `awayTeamRuns`, etc.) are assumed based on the `games_wide` schema — adjust in staging models if they differ.
5. Red Sox-specific filtering is applied only at the `agg_redsox_performance` level; all base tables retain all teams.
6. Dev dataset (`baseball_dev`) is used for safe testing before promoting to production.
7. Division mappings in `dim_teams` reflect current MLB structure and may need updating for historical data.
8. Team names are standardized to short versions (e.g., 'Red Sox' instead of 'Boston Red Sox').

---

## Project Structure

```
baseball_dataplatform/
├── dbt_project.yml
├── packages.yml
├── profiles.yml              ← copy to ~/.dbt/profiles.yml
├── README.md
├── models/
│   ├── raw/
│   │   ├── _raw__sources.yml
│   │   ├── raw_games_wide.sql
│   │   ├── raw_schedules.sql
│   │   └── raw_games_post_wide.sql
│   ├── staging/
│   │   ├── _staging__models.yml
│   │   ├── stg_games.sql
│   │   ├── stg_teams.sql
│   │   ├── stg_schedules.sql
│   │   └── stg_postseason.sql
│   └── mart/
│       ├── _mart__models.yml
│       ├── dimensions/
│       │   ├── dim_teams.sql
│       │   └── dim_dates.sql
│       ├── facts/
│       │   ├── fct_games.sql
│       │   └── fct_postseason_games.sql
│       ├── aggregates/
│       │   ├── agg_team_season_stats.sql
│       │   ├── agg_redsox_performance.sql
│       │   └── agg_team_stats.sql
│       └── dashboard/
│           ├── dash_redsox_overview.sql
│           ├── dash_team_analysis.sql
│           ├── dash_league_comparison.sql
│           ├── dash_game_analysis.sql
│           └── dash_postseason_summary.sql
├── tests/
│   ├── assert_no_negative_runs.sql
│   └── assert_win_pct_valid_range.sql
├── macros/
│   └── generate_schema_name.sql
├── analyses/
│   └── baseball_exploratory.sql
└── scripts/
    ├── setup_gcp.sh
    ├── share_access.sh
    └── snapshot_tables.sh
```
