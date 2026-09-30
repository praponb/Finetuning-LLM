from pyspark.sql import functions as F

filtered = (
    df_promotions
    .filter(F.col("channel").isin(["web", "store", "partner"]))
    .select("*", F.substring("channel", 1, 2).alias("channel_prefix"))
)

result = (
    filtered
    .groupBy("promo_code")
    .pivot("channel", ["web", "partner"])
    .agg(F.max("discount_pct").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
