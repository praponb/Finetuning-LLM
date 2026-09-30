from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "level",
        "k",
        F.when(F.col("k").rlike("^(trace|region)$"), "GROUP_B").when(F.col("k").rlike("^(version)$"), "EXTENDED").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").desc(), F.col("level").desc())
)
