# HCM Analytics POV: Databricks Medallion Pipeline

Oracle HCM extracts (9 CSVs with 1,786 known defects) → Bronze → Silver → Gold star schema on Databricks, orchestrated as one Lakeflow Job. Answers headcount, attrition and learning questions, with every defect logged and scored against an answer key.

## Layout

```
config/
  config.yaml          business rules (dates, null handling, domains, snapshot range)
  config.json          same config, used automatically if pyyaml is unavailable; keep in sync
src/
  hcm_utils.py         shared helpers (plain module, imported by the notebooks)
  00_setup             schemas, volumes, DQ tables; moves uploaded files into place   (run once)
  01_bronze_ingest     Auto Loader → 9 bronze tables, all STRING + audit columns
  02_silver_worker     → silver.hcmworker            (SCD2)
  03_silver_assignment → silver.hcmassignment        (dedup, fixes, DqFlags)
  04_silver_periods_of_service → silver.hcmperiodsofservice
  05_silver_job_grade  → silver.hcmjob, silver.hcmgrade (SCD2)
  06_silver_learning   → silver.hcmlearningactivity
  07_gold_dimensions   7 dimensions
  08_gold_facts        headcount, attrition, learning facts
  09_dq_reconcile      scores dq_issue_log vs answer key; fails if recall < 100%
  10_security_pii      masks, row filter, tags                                         (run once, after Gold)
  99_validate_checkpoints  asserts every expected row count and KPI
resources/hcm_job.yml  Lakeflow Job definition (Asset Bundle)
databricks.yml         bundle root
```

## Run it

1. **Git folder:** in Databricks, *Workspace → Create → Git folder* with this repo's URL.
2. **Setup:** open `src/00_setup` and run it once. It creates the schemas and volumes, then stops with a list of missing files.
3. **Upload data:** in Catalog Explorer, upload the 9 CSVs and `DQ_ANSWER_KEY.json` into `/Volumes/workspace/landing/hcm_raw/_upload/`, then re-run `00_setup`. It moves each file into its table folder.
4. **Pipeline:** either run notebooks `01` → `06` → `09` → `07` → `08` by hand, or create the job:
   - UI: *Jobs & Pipelines → Create → Job*, with tasks and dependencies as in `resources/hcm_job.yml`, job parameters `catalog = workspace` and `run_id = {{job.run_id}}`, serverless compute.
   - CLI: set your host in `databricks.yml`, then `databricks bundle deploy -t dev && databricks bundle run hcm_medallion -t dev`.
5. **Check:** run `src/99_validate_checkpoints`. Every row should be ✅.
6. **Security (optional):** run `src/10_security_pii`.

```
bronze_ingest → silver_worker | silver_assignment | silver_periods | silver_job_grade | silver_learning
              → dq_reconcile (gate) → gold_dimensions → gold_facts
```

## Key checkpoints

| Layer | Expected |
|---|---|
| Bronze total | 26,168 rows |
| Silver | worker 2,693 · assignment 2,705 · periods 2,688 · job 60 · grade 12 · learning 17,923 |
| DQ | 1,786 / 1,786 defects matched |
| Gold facts | headcount 116,999 · attrition 688 · learning 17,923 |
| KPIs | headcount 2,005 at 2026-06-30 · 2025 attrition 14.4% · mandatory compliance 81.4% |

## Notes

- **Re-runs:** Bronze uses Auto Loader checkpoints, so a re-run with no new files adds nothing. To start fresh, drop the bronze tables and delete `/Volumes/workspace/landing/checkpoints/`.
- **Running interactively:** each notebook defaults to `run_id = manual`. `09_dq_reconcile` then scores the most recent run, so run all five Silver notebooks before it.
- **Free Edition:** serverless only, max 5 concurrent tasks (set max concurrent runs = 1), and a daily usage quota, so trigger the job manually. Delete any test roles in `security.user_roles` before running the job.
- **Data:** the CSVs and the answer key are git-ignored. Never commit employee data.
