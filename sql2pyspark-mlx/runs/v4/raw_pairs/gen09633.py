from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "log_ts",
        "key",
        "attr_value",
        F.when(F.col("service").rlike("^(search|auth)$"), "CORE").otherwise("GROUP_A").alias("service_zone"),
    )
    .orderBy(F.col("id").desc())
)
