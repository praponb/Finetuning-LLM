from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "value",
        F.when(F.col("host").rlike("^(h2|h3)$"), "GROUP_A").when(F.col("host").rlike("^(h1|h4)$"), "EXTENDED").otherwise("PRIORITY").alias("host_zone"),
    )
)
