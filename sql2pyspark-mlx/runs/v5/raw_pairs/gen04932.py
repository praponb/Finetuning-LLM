from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("country", "id")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([4, 23, 36]))
        .select("country", "id")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("browser")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("started_at").alias("ended_at"),
        F.max("started_at").alias("last_seen"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
