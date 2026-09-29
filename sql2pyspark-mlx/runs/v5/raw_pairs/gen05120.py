from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.when(F.col("currency").rlike("^(EUR|USD)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(THB)$"), "PRIORITY").otherwise("EXTENDED").alias("currency_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
