from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = flattened.select("latency_ms", "attr_key", "value")
