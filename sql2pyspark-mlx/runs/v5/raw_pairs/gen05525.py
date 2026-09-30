from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.min("element").alias("min_element"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
)
