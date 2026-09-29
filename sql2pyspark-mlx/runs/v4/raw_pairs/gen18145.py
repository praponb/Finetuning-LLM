from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2030, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["latency_ms_tier", "host"], ["latency_ms_tier"], []],
        "latency_ms_tier",
        "host",
    )
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.max(F.when(F.col("host") == "h1", F.col("duration_sec"))).alias("h1_max_duration_sec"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.size(F.collect_set("host")).alias("num_hosts_2"),
        F.max("latency_ms").alias("max_latency_ms_2"),
    )
    .filter(F.col("num_hosts") >= 1)
)
