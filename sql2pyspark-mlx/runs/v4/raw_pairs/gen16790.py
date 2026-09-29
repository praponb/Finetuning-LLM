from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "service")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("attr_key")
w4 = Window.partitionBy("service", "level")
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code_percent_rank").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
    )
)

scored = (
    metrics
    .withColumn("corr_status_code_latency_ms_2", F.corr("status_code", "latency_ms").over(w4))
    .withColumn("status_code_percent_rank_cume_dist", F.cume_dist().over(w5))
)

enriched = (
    scored
    .withColumn("corr_status_code_latency_ms_3", F.corr("status_code", "latency_ms").over(w3))
    .withColumn("attr_key_stddev_samp_status_code_percent_rank_cume_dist", F.stddev_samp("status_code_percent_rank_cume_dist").over(w3))
)

result = (
    enriched
    .groupBy(
        "level",
        F.when(F.col("status_code") >= 441, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("attr_key") == "trace", F.col("status_code_percent_rank"))).alias("trace_total_status_code_percent_rank"),
        F.countDistinct("service").alias("distinct_services"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
)
