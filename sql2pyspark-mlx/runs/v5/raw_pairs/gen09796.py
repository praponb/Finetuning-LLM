from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

result = (
    lines
    .select(
        "id",
        "currency",
        "val",
        F.when(F.col("currency").rlike("^(EUR|USD)$"), "PRIORITY").when(F.col("currency").rlike("^(THB)$"), "EXTENDED").otherwise("GROUP_A").alias("currency_zone"),
    )
    .orderBy("id")
)
