from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "attr_key",
        F.when(F.col("service").rlike("^(search|checkout)$"), "PRIORITY").when(F.col("service").rlike("^(auth|billing)$"), "EXTENDED").otherwise("GROUP_B").alias("service_zone"),
    )
)
