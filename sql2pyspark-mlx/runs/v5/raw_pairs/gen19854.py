from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "service", "log_ts", "status_code", "attr_value")
    .orderBy(F.col("id").desc(), "service")
)
