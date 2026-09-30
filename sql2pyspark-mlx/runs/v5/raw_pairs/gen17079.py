from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 61, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.min("element").alias("min_element"),
        F.avg("element").alias("avg_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("element_per_thb"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
    )
    .filter(F.col("element_per_thb") > 0)
    .orderBy(F.col("element_per_thb").desc_nulls_first())
)
