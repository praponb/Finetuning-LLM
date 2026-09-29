from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("id", "country")
    .subtract(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("id", "country")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "customer_id", "device", "browser", "started_at")
    .orderBy(F.col("id").desc_nulls_last())
)
