from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("host", "attr_key").orderBy("service_max_latency_ms")
w4 = Window.partitionBy("service", "attr_key")
w5 = Window.partitionBy("service").orderBy("service_max_latency_ms")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    lines
    .withColumn("host_min_latency_ms", F.min("latency_ms").over(w))
    .withColumn("service_max_latency_ms", F.max("latency_ms").over(w2))
)

metrics = (
    with_windows
    .withColumn("service_max_latency_ms_percent_rank", F.percent_rank().over(w3))
    .withColumn("host_min_latency_ms_share", F.round((F.col("host_min_latency_ms") / F.sum("host_min_latency_ms").over(w4)), 4))
    .withColumn("service_max_latency_ms_cume_dist", F.cume_dist().over(w5))
)

result = (
    metrics
    .groupBy("service")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("service").alias("distinct_services"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
