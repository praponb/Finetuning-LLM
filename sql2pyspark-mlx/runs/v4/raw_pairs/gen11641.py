from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k").orderBy("status_code", "id", "k")
w2 = Window.orderBy("dedup_rank", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("status_code", "id", "k").rowsBetween(-3, -1)
w4 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("k").isNotNull() & ~F.col("host").isin(["h2", "h3", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "k",
        "value",
        F.col("status_code").cast("string").alias("status_code_num"),
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("status_code").over(w2).alias("running_status_code"),
        F.avg("latency_ms_filled").over(w3).alias("latency_ms_filled_avg_3_prior"),
        F.count("latency_ms_filled").over(w4).alias("service_count_latency_ms_filled"),
    )
)

result = (
    metrics
    .groupBy("service")
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.round(F.sum("service_count_latency_ms_filled") / F.nullif(F.count(F.when(F.col("k") == "trace", 1)), F.lit(0)), 2).alias("service_count_latency_ms_filled_per_trace"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
)
