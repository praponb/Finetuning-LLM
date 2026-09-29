from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)
