from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") != "checkout")
    .select(
        "id",
        "service",
        "log_ts",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.when(F.col("duration_sec") >= 3.8, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.sum(F.when(F.col("service") == "billing", F.col("duration_sec"))).alias("billing_total_duration_sec"),
    )
)
