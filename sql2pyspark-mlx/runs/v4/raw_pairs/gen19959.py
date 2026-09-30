from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "latency_ms", "status_code", F.explode("context").alias("k", "value"))
)

result = (
    flattened
    .select(
        "id",
        "status_code",
        "k",
        "value",
        F.when(F.col("k").rlike("^(trace|region)$"), "PRIORITY").when(F.col("k").rlike("^(version)$"), "GROUP_A").otherwise("CORE").alias("k_zone"),
    )
    .orderBy("k")
)
