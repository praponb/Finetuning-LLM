from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("latency_ms_bit_or").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 462, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        "service",
        "level",
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count("*").alias("num_rows"),
        F.countDistinct("key").alias("distinct_keys"),
    )
    .withColumn("latency_ms_bit_or_percent_rank", F.percent_rank().over(w))
)
