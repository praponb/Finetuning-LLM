from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("status_code_high").desc())
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key").rowsBetween(-5, -1)
w6 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy("dedup_rank", "id", "attr_key")
w8 = Window.partitionBy("attr_key").orderBy("seq_no_avg_5_prior", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("attr_key").orderBy("seq_no", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "status_code",
        "attr_key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
    )
)

enriched = (
    parsed
    .select(
        "*",
        F.last("attr_key", ignorenulls=True).over(w).alias("last_known_attr_key"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched_app_logs = (
    ranked
    .select(
        "*",
        F.count("status_code_high").over(w4).alias("attr_key_count_status_code_high"),
        F.avg("seq_no").over(w5).alias("seq_no_avg_5_prior"),
        F.first("seq_no", ignorenulls=True).over(w).alias("first_known_seq_no"),
        F.ntile(10).over(w6).alias("status_code_bucket"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    enriched_app_logs
    .withColumn("running_status_code_bucket", F.sum("status_code_bucket").over(w))
    .withColumn("running_status_code", F.sum("status_code").over(w8))
    .withColumn("cumulative_seq_no_avg_5_prior", F.sum("seq_no_avg_5_prior").over(w9))
)
