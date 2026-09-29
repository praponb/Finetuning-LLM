from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "key",
        F.when(F.col("service").rlike("^(checkout|billing)$"), "PRIORITY").when(F.col("service").rlike("^(auth|search)$"), "EXTENDED").otherwise("CORE").alias("service_zone"),
    )
    .orderBy("host", "status_code")
)
