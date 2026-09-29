from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 64, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_total_element"),
    )
    .orderBy(F.col("eur_total_element").desc())
)
