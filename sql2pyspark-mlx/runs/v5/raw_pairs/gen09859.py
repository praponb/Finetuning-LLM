from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        F.when(F.col("host").rlike("^(h3|h2)$"), "GROUP_A").when(F.col("host").rlike("^(h1|h4)$"), "PRIORITY").otherwise("CORE").alias("host_zone"),
    )
)
