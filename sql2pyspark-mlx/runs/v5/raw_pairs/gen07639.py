from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("host")
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.min("status_code").alias("min_status_code"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.sum(F.when(F.col("host") == "h3", F.col("latency_ms")).otherwise(0)).alias("h3_latency_ms"),
    )
)
