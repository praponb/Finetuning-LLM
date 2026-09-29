from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

result = (
    exploded
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(USD)$"), "GROUP_B").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
