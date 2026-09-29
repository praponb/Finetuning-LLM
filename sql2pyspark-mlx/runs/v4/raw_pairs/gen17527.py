from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = lines.select("id", "log_ts", "level", "status_code", "key", "attr_value")
