from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = flattened.select("service", "status_code", "attr_key", "attr_value")
