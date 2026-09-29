from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 74, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
        F.date_trunc("quarter", "effective_from").alias("quarter_start"),
    )
    .agg(
        F.sum("element").alias("total_element"),
        F.sum("element").alias("total_element_2"),
    )
    .orderBy(F.col("element_tier").asc_nulls_last())
)
