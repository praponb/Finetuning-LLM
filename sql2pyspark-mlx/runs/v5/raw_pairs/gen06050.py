from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select("id", "host", "log_ts", "level", "attr_key")
    .orderBy("log_ts")
)
