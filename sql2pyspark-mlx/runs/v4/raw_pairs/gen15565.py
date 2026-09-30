from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    lines
    .select(
        "id",
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "EXTENDED").when(F.col("currency").rlike("^(THB)$"), "CORE").otherwise("GROUP_B").alias("currency_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
