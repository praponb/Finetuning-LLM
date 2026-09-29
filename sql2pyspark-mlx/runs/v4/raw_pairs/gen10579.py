from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("country")
    .exceptAll(
        df_sessions
        .filter(F.col("started_at") >= "2023-08-01")
        .select("country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "customer_id",
        "started_at",
        "country",
        F.when(F.col("country").rlike("^(US|DE)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy("started_at")
)
