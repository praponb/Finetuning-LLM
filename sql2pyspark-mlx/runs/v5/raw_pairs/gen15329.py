from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    lines
    .select(
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "OTHER_ZONE").when(F.col("currency").rlike("^(EUR)$"), "EXTENDED").otherwise("GROUP_B").alias("currency_zone"),
    )
    .orderBy(F.col("currency_zone").asc_nulls_last())
)
