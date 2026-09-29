from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "host",
        "latency_ms",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2546, "LARGE").otherwise("SMALL"))
    .cube("host", "latency_ms_tier")
    .agg(
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms")).otherwise(0)).alias("h1_latency_ms"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.grouping("host").alias("gid"),
    )
)
