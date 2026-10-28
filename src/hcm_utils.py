"""Shared helpers for the HCM medallion pipeline.

Final consolidated version of the helpers introduced across modules 04, 06 and 07.
Upload to src/hcm_utils.py in your Git folder; notebooks in src/ import from it.
"""
import json
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

END_OF_TIME_TS = "9999-12-31 00:00:00"


def _spark() -> SparkSession:
    # Resolved at call time (not import time) so the module also works in unit tests.
    return SparkSession.getActiveSession()


# ---------------------------------------------------------------- config
def load_config(path: str = "../config/config.yaml") -> dict:
    """Load config.yaml; fall back to config.json when pyyaml is unavailable (Free Edition)."""
    try:
        import yaml
    except ImportError:
        path = os.path.splitext(path)[0] + ".json"
        with open(os.path.abspath(path)) as f:
            return json.load(f)
    with open(os.path.abspath(path)) as f:
        return yaml.safe_load(f)


def resolve_paths(cfg: dict, catalog: str) -> dict:
    """Fill the {catalog} placeholder in config paths, so one config serves dev/test/prod."""
    return {k: v.format(catalog=catalog) for k, v in cfg["paths"].items()}


# ---------------------------------------------------------------- parsing
def parse_date(col_name: str):
    """Parse ISO, MM/dd/yyyy and dd-MMM-yyyy (Oracle) into DATE. NULL if no format matches."""
    c = F.trim(F.col(col_name))
    return F.coalesce(
        F.to_date(F.try_to_timestamp(F.substring(c, 1, 10), F.lit("yyyy-MM-dd"))),  # also drops ' 00:00:00'
        F.to_date(F.try_to_timestamp(c, F.lit("MM/dd/yyyy"))),
        F.to_date(F.try_to_timestamp(c, F.lit("dd-MMM-yyyy"))),
    )


def to_int(col_name: str):
    """'3002.0' -> 3002 ; NULL / non-numeric -> NULL."""
    return F.expr(f"try_cast(try_cast({col_name} AS DOUBLE) AS INT)")


# ---------------------------------------------------------------- bronze
def latest_batch(table: str) -> DataFrame:
    """Rows from the most recent Bronze load of a table (full-snapshot extracts only; see module 13)."""
    df = _spark().table(table)
    last = df.agg(F.max("BronzeLoadTimestamp")).first()[0]
    return df.filter(F.col("BronzeLoadTimestamp") == F.lit(last))


# ---------------------------------------------------------------- DQ logging
def issue_rows(df: DataFrame, source_file: str, ref_col: str, issue_type: str,
               description: str, action: str) -> DataFrame:
    """Shape offending rows into dq_issue_log format (RowRef matches DQ_ANSWER_KEY row_ref)."""
    return df.select(
        F.lit(source_file).alias("SourceFile"),
        F.concat(F.lit(f"{ref_col}="), F.col(ref_col).cast("string")).alias("RowRef"),
        F.lit(issue_type).alias("IssueType"),
        F.lit(description).alias("Description"),
        F.lit(action).alias("ActionTaken"),
    )


def write_issues(issue_dfs: list, catalog: str, run_id: str) -> int:
    if not issue_dfs:
        return 0
    out = issue_dfs[0]
    for d in issue_dfs[1:]:
        out = out.unionByName(d)
    out = (out.withColumn("RunId", F.lit(run_id))
              .withColumn("DetectedAt", F.current_timestamp()))
    out.write.mode("append").saveAsTable(f"{catalog}.silver.dq_issue_log")
    return out.count()


def quarantine(df: DataFrame, source_file: str, ref_col: str, reason: str,
               catalog: str, run_id: str) -> None:
    (df.select(
        F.lit(source_file).alias("SourceFile"),
        F.concat(F.lit(f"{ref_col}="), F.col(ref_col).cast("string")).alias("RowRef"),
        F.lit(reason).alias("Reason"),
        F.to_json(F.struct(*df.columns)).alias("RawRecord"),
        F.lit(run_id).alias("RunId"),
        F.current_timestamp().alias("QuarantinedAt"))
     .write.mode("append").saveAsTable(f"{catalog}.silver.quarantine"))


# ---------------------------------------------------------------- silver writes
def upsert(df: DataFrame, table: str, keys: list) -> None:
    """Type-1 MERGE (insert new, overwrite changed). Creates the table on first run."""
    spark = _spark()
    if not spark.catalog.tableExists(table):
        df.write.saveAsTable(table)
        return
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def scd2_merge(src: DataFrame, table: str, key: str, tracked_cols: list) -> None:
    """Pipeline-managed SCD Type 2 on a single business key."""
    spark = _spark()
    src = (src
        .withColumn("RowHash", F.sha2(F.concat_ws("||", *[
            F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in tracked_cols]), 256))
        .withColumn("EffectiveFrom", F.current_timestamp())
        .withColumn("EffectiveTo", F.lit(END_OF_TIME_TS).cast("timestamp"))
        .withColumn("IsCurrent", F.lit(True)))

    if not spark.catalog.tableExists(table):
        src.write.saveAsTable(table)
        return

    tgt = DeltaTable.forName(spark, table)
    current = tgt.toDF().filter("IsCurrent")
    changed = (src.alias("s").join(current.alias("t"), key)
                  .filter("s.RowHash <> t.RowHash").select("s.*"))

    key_type = src.schema[key].dataType
    staged = (changed.withColumn("_mk", F.lit(None).cast(key_type))   # inserts the new version
              .unionByName(src.withColumn("_mk", F.col(key))))        # expires old version / inserts new keys

    (tgt.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s._mk AND t.IsCurrent = true")
        .whenMatchedUpdate(condition="t.RowHash <> s.RowHash",
                           set={"IsCurrent": "false", "EffectiveTo": "s.EffectiveFrom"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in src.columns})
        .execute())


# ---------------------------------------------------------------- gold writes
def rebuild(table: str, select_sql: str) -> int:
    """Full-refresh a Gold table while keeping its UC metadata (masks, tags, grants)."""
    spark = _spark()
    if spark.catalog.tableExists(table):
        spark.sql(f"INSERT OVERWRITE {table} {select_sql}")
    else:
        spark.sql(f"CREATE TABLE {table} AS {select_sql}")
    return spark.table(table).count()
