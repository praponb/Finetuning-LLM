from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("element"))

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 88, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
        "currency",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.count("*").alias("num_rows"),
        F.avg("element").alias("avg_element"),
    )
)
