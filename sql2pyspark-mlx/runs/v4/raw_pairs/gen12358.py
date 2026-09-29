from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("element") >= 87, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("usd_rows").desc())
)
