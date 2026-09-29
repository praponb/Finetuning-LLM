from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("has_fault", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w3 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("attr_key", "host").orderBy("status_code", "id", "attr_key").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("host", "attr_key").orderBy("last_known_has_fault", "id", "attr_key")
w6 = Window.partitionBy("attr_key").orderBy(F.col("next_status_code_high").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "host",
        "status_code",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

with_windows = (
    parsed
    .select(
        "*",
        F.ntile(10).over(w).alias("has_fault_bucket"),
        F.ntile(3).over(w2).alias("status_code_bucket"),
        F.last("has_fault", ignorenulls=True).over(w3).alias("last_known_has_fault"),
        F.avg("status_code").over(w_recent).alias("status_code_avg_6"),
    )
)

enriched = (
    with_windows
    .filter(F.col("last_known_has_fault") >= 1)
    .select(
        "id",
        "status_code",
        "attr_key",
        "status_code_high",
        "has_fault_bucket",
        "last_known_has_fault",
        F.lead("status_code").over(w5).alias("next_status_code"),
        F.lag("status_code_high").over(w5).alias("prev_status_code_high"),
        F.lead("status_code_high").over(w5).alias("next_status_code_high"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "attr_key", "last_known_has_fault", "next_status_code_high")
)

result = (
    ranked
    .select(
        "id",
        "attr_key",
        "last_known_has_fault",
        F.when(F.col("attr_key").rlike("^(region|trace)$"), "GROUP_A").when(F.col("attr_key").rlike("^(version)$"), "CORE").otherwise("PRIORITY").alias("attr_key_zone"),
    )
)
