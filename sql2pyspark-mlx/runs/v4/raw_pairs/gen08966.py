from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("billing_max_latency_ms").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy("level", "host")
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.max(F.when(F.col("service") == "billing", F.col("latency_ms"))).alias("billing_max_latency_ms"),
    )
    .filter(F.col("billing_max_latency_ms") > 0)
    .withColumn("billing_max_latency_ms_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("host").desc_nulls_last())
)
