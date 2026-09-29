from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

result = (
    flattened
    .select(
        "id",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
)
