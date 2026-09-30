from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)
