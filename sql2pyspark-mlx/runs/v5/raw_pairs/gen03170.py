from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"), "currency")
    .agg(
        F.avg(F.when(F.col("currency") == "USD", F.col("element"))).alias("usd_avg_element"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("element_per_thb"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
)
