from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("host").isin(["h4", "h1"]))
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("status_code") >= 462, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.countDistinct("user_tier").alias("distinct_user_tiers"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code")).otherwise(0)).alias("h4_status_code"),
        F.min("status_code").alias("min_status_code"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
