from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "service",
        "status_code",
        "k",
        "value",
        F.when(F.col("k").rlike("^(trace|region)$"), "CORE").when(F.col("k").rlike("^(version)$"), "EXTENDED").otherwise("GROUP_A").alias("k_zone"),
    )
    .orderBy("id", F.col("status_code").desc())
)
