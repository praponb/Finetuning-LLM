from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .withColumn("retry_count_tier", F.when(F.col("retry_count") >= 4, "PREMIUM").otherwise("STANDARD"))
    .rollup("retry_count_tier")
    .agg(
        F.round(F.avg("has_fault"), 2).alias("avg_has_fault"),
        F.count("*").alias("cnt"),
        F.grouping("retry_count_tier").alias("grouping_level"),
    )
    .orderBy("avg_has_fault")
)
