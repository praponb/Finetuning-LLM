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
        F.when(F.col("element") >= 62, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_avg_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
)
