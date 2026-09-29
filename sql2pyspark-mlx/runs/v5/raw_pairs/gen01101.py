from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

result = (
    exploded
    .select(
        "id",
        "currency",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "GROUP_A").when(F.col("currency").rlike("^(THB)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy("currency", "currency_zone")
)
