from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("duration_sec") >= 3.4, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "billing", F.col("status_code"))).alias("billing_total_status_code"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count_if(F.col("tier") == "free").alias("free_events"),
    )
    .filter(F.col("row_count") < 1000)
    .orderBy("row_count")
)
