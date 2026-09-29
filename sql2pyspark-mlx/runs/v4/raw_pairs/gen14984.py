from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select("id", "service", "attr_key", "attr_value")
    .orderBy("attr_key", F.col("id").desc())
)
