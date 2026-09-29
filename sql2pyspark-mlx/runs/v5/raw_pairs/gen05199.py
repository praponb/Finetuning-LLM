from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("latency_ms", "id", "k")
w_running_2 = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("status_code", "id", "k")
w_part = Window.partitionBy("service", "k").orderBy("running_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("k", "service").orderBy(F.col("latency_ms").desc_nulls_first())
w6 = Window.partitionBy("service").orderBy(F.col("status_code_bucket").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

metrics = (
    exploded
    .withColumn("latency_ms_bucket", F.ntile(4).over(w_running))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w_running_2))
    .withColumn("status_code_bucket", F.ntile(5).over(w3))
)

enriched = (
    metrics
    .withColumn("cumulative_latency_ms_bucket", F.sum("latency_ms_bucket").over(w_part))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w5))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        "latency_ms_bucket",
        "running_latency_ms",
        "status_code_bucket",
        "cumulative_latency_ms_bucket",
        "latency_ms_cume_dist",
    )
    .orderBy(F.col("k").asc_nulls_first())
)
