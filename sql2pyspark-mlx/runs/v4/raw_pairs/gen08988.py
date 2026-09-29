from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "k",
        "attr_value",
        F.when(F.col("k").rlike("^(version|region)$"), "GROUP_A").when(F.col("k").rlike("^(trace)$"), "EXTENDED").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("k").desc())
)
