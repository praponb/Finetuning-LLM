from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 465, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms")).otherwise(0)).alias("h1_latency_ms"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
)
