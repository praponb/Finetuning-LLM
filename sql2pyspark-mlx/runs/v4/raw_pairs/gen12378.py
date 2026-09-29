from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level").isin(["INFO", "WARN"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "payload_json",
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
