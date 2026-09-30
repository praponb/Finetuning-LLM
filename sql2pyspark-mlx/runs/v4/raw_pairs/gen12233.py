from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("device", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("device") != "desktop")
        .select("device", "customer_id")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "customer_id")
    .orderBy(F.col("customer_id").desc_nulls_last(), "id")
)
