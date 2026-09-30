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
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "CORE").when(F.col("currency").rlike("^(USD)$"), "GROUP_A").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy("id")
)
