from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("customer_id", "country")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["JP", "DE"]))
        .select("customer_id", "country")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "browser",
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "country",
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("distinct_customer_ids") >= 1)
)
