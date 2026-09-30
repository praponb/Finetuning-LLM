from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "service",
        "attr_key",
        F.when(F.col("service").rlike("^(auth|checkout)$"), "OTHER_ZONE").when(F.col("service").rlike("^(billing|search)$"), "CORE").otherwise("EXTENDED").alias("service_zone"),
    )
)
