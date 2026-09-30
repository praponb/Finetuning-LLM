from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "key").rowsBetween(-6, -1)
w_running = Window.partitionBy("key")
w3 = Window.partitionBy("host", "service").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service", "key").orderBy(F.col("has_fault").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

flattened_attrs = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    flattened_attrs
    .withColumn("status_code_avg_6_prior", F.avg("status_code").over(w))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w_running))
    .withColumn("cumulative_status_code_high", F.sum("status_code_high").over(w3))
    .withColumn("has_fault_dense_rank", F.dense_rank().over(w4))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("has_fault") >= 1, "HIGH").otherwise("LOW").alias("has_fault_tier"),
    )
    .agg(
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.sum(F.when(F.col("key") == "trace", F.col("has_fault_dense_rank")).otherwise(0)).alias("trace_has_fault_dense_rank"),
        F.count("*").alias("cnt"),
        F.percentile_approx("status_code_avg_6_prior", [0.25, 0.5, 0.75]).alias("status_code_avg_6_prior_quartiles"),
    )
    .filter(F.col("cnt") < 1)
    .orderBy(F.col("avg_status_code_high").desc_nulls_first())
)
