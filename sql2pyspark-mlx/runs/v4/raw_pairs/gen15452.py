from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level")
w2 = Window.orderBy(F.col("is_flagged").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

parsed = (
    lines
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "level",
        "status_code",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    parsed
    .select(
        "*",
        F.sum("is_flagged").over(w_running).alias("level_sum_is_flagged"),
        F.percent_rank().over(w2).alias("is_flagged_percent_rank"),
    )
)

daily = (
    enriched
    .rollup("level")
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.grouping("level").alias("grouping_id"),
    )
)

result = (
    daily
    .select("level", "warn_rows", "grouping_id")
    .orderBy("warn_rows")
)
