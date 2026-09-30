from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("duration_sec") >= 3.7, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_count_2"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
    .orderBy(F.col("h4_events").desc_nulls_last())
)
