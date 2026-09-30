from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .withColumn("element_tier", F.when(F.col("element") >= 84, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["currency", "year_start", "element_tier"], ["currency", "year_start"], ["currency"]],
        "currency",
        "year_start",
        "element_tier",
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count("*").alias("num_rows"),
    )
)
