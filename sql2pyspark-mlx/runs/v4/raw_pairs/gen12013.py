from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "status_code",
        "key",
        "attr_value",
        F.when(F.col("service").rlike("^(billing|auth)$"), "EXTENDED").when(F.col("service").rlike("^(search|checkout)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy(F.col("service").asc_nulls_first(), "key")
)
