from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code", "id", "k")
w3 = Window.partitionBy("service").orderBy("running_max_latency_ms", "id", "k").rowsBetween(-5, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

metrics = (
    exploded
    .filter(~F.col("level").isin(["WARN", "INFO", "ERROR"]))
    .select(
        "*",
        F.avg("latency_ms").over(w_part).alias("running_avg_latency_ms"),
        F.sum("latency_ms").over(w_part).alias("running_sum_latency_ms"),
        F.max("latency_ms").over(w_part).alias("running_max_latency_ms"),
    )
)

with_windows = (
    metrics
    .withColumn("prev_running_max_latency_ms", F.lag("running_max_latency_ms").over(w2))
    .withColumn("next_running_sum_latency_ms", F.lead("running_sum_latency_ms").over(w2))
    .withColumn("prev_running_sum_latency_ms", F.lag("running_sum_latency_ms").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("latency_ms_avg_5_prior", F.avg("latency_ms").over(w3))
)

cleaned = (
    with_windows
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (F.col("running_avg_latency_ms") < 2100))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "v",
        "running_avg_latency_ms",
        "prev_running_max_latency_ms",
        "prev_running_sum_latency_ms",
        "row_seq",
        "latency_ms_avg_5_prior",
        F.when(F.col("level") == "ERROR", 1).otherwise(0).alias("is_error"),
        F.round((F.col("prev_running_sum_latency_ms") * 100), 2).alias("adjusted_prev_running_sum_latency_ms"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "k",
        "running_avg_latency_ms",
        "prev_running_max_latency_ms",
        "row_seq",
        "is_error",
        F.when(F.col("k").rlike("^(version|region)$"), "GROUP_B").when(F.col("k").rlike("^(trace)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("k_zone"),
    )
)
