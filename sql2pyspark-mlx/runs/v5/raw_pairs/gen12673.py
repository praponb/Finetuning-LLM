from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["search", "checkout"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
    )
)
