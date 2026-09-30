from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

prev_events = flattened.withColumn("prev_log_ts", F.lag("log_ts").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "k",
        "value",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

valid = (
    islands
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "status_code",
        "k",
        F.upper("service").alias("service_upper"),
        F.greatest("burst_id", "status_code").alias("greatest_burst_id_status_code"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

cleaned = (
    valid
    .filter(F.col("k").isNotNull())
    .withColumn("k_length", F.length("k"))
)

result = (
    cleaned
    .groupBy("k", "service")
    .agg(
        F.sum(F.when(F.col("k") == "version", F.col("k_length"))).alias("version_total_k_length"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count_if(F.col("service") == "auth").alias("auth_events"),
    )
    .orderBy(F.col("num_ks").desc_nulls_last())
)
