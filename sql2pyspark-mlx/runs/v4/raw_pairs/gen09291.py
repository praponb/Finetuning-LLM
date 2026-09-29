from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("is_flagged", "id", "k")
w2 = Window.orderBy("is_flagged", "id", "k")
w3 = Window.orderBy("status_code_high")
w4 = Window.partitionBy("k")
w_ordered = Window.partitionBy("k").orderBy("is_flagged_bucket", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("status_code").between(393, 413))
    .select(
        "id",
        "status_code",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

enriched = (
    flattened_attrs
    .select(
        "*",
        F.ntile(3).over(w).alias("is_flagged_bucket"),
        F.lag("status_code_high", 1).over(w2).alias("prev_status_code_high"),
        F.cume_dist().over(w3).alias("status_code_high_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4).alias("status_code_share"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("is_flagged").over(w_ordered).alias("cumulative_is_flagged"),
        F.row_number().over(w).alias("event_seq"),
    )
)

totals = (
    with_windows
    .groupBy("k")
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code"))).alias("version_total_status_code"),
    )
)

result = (
    totals
    .select("k", "version_count", "version_total_status_code")
    .orderBy(F.col("k").desc_nulls_last(), "version_count")
    .limit(5)
)
