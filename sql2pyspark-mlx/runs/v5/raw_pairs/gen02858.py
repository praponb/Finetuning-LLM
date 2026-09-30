from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode("discounts").alias("element"))

result = (
    exploded
    .groupBy(
        "currency",
        F.when(F.col("element") >= 68, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_avg_element"),
        F.percentile_approx("element", 0.9).alias("element_p90"),
    )
)
