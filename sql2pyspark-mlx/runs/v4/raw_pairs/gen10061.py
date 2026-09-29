from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    lines
    .select("id", "service", "log_ts", "level", "status_code", "k")
    .orderBy("log_ts")
)
