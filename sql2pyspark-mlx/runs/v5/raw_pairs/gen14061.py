from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("key", "v"),
    )
)

valid = (
    exploded
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .select("service", "host", "log_ts", "key", F.hour("log_ts").alias("log_ts_hour"))
)

cleaned = (
    valid
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("trace"))))
    .select(
        "service",
        "host",
        "key",
        F.when(F.col("host").isin(["h1", "h4"]), "Core").otherwise("Other").alias("host_group"),
        F.coalesce("log_ts_hour", F.lit(0)).alias("log_ts_hour_filled"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select("service", "host_group", F.trim("host").alias("clean_host"))
)

valid_app_logs = (
    cleaned_app_logs
    .filter(F.col("service") == "auth")
    .select("*", F.trim("service").alias("clean_service"))
)

grouped = (
    valid_app_logs
    .groupBy("service")
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.countDistinct("service").alias("distinct_services"),
    )
)

result = (
    grouped
    .select("service", "billing_count", "auth_rows")
    .orderBy("billing_count")
)
