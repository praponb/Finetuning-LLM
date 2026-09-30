from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

result = (
    lines
    .select("id", "currency", "element")
    .orderBy(F.col("element").desc(), "id")
)
