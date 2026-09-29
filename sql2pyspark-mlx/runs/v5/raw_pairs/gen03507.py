from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("element"))
)

result = (
    lines
    .select(
        "id",
        "currency",
        "element",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "CORE").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy("currency_zone")
)
