from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .select(
        "id",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "GROUP_B").when(F.col("currency").rlike("^(USD)$"), "OTHER_ZONE").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)
