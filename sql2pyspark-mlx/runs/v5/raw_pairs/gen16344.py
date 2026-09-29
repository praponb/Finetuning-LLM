from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("latency_ms_tier").orderBy(F.col("trace_max_status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 1890, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        F.when(F.col("status_code") >= 417, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.max(F.when(F.col("key") == "trace", F.col("latency_ms"))).alias("trace_max_latency_ms"),
        F.count_if(F.col("key") == "region").alias("region_events"),
        F.sum("latency_ms").alias("total_latency_ms"),
        F.max(F.when(F.col("key") == "trace", F.col("status_code"))).alias("trace_max_status_code"),
    )
    .filter(F.col("total_latency_ms") > 0)
    .withColumn("trace_max_status_code_rank", F.rank().over(w))
)
