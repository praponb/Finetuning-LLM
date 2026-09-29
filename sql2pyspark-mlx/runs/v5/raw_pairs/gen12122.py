from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

result = (
    lines
    .select("id", "plan", "val")
    .orderBy(F.col("plan").desc())
)
