from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = exploded.select("id", "host", "status_code", "attr_key", "attr_value")
