from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("country")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([5, 18, 24, 29]))
        .select("country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.min("started_at").alias("started_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)
