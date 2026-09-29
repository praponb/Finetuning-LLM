from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)
