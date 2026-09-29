from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("browser", "device")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["UK", "SG"]))
        .select("browser", "device")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.max("started_at").alias("last_seen"),
        F.max("started_at").alias("last_seen_2"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
