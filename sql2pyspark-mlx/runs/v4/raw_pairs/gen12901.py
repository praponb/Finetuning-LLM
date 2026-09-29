from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "host", "status_code", F.explode("context").alias("k", "value"))
)

valid = (
    lines
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "host",
        "k",
        "value",
        F.when(F.col("status_code") >= 412, "High").when(F.col("status_code") >= 278, "Medium").otherwise("Low").alias("status_code_level"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

valid_app_logs = (
    valid
    .filter(~F.col("k").isin(["region", "version"]))
    .select(
        "*",
        F.coalesce("k", F.lit("Unknown")).alias("k_filled_2"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

result = (
    valid_app_logs
    .select(
        "id",
        "k",
        "value",
        "status_code_level",
        "k_filled",
        F.when(F.col("k").rlike("^(trace|region)$"), "GROUP_B").when(F.col("k").rlike("^(version)$"), "EXTENDED").otherwise("CORE").alias("k_zone"),
    )
    .orderBy(F.col("k_zone").asc_nulls_first())
)
