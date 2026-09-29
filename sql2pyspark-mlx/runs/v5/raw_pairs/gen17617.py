from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .pivot("channel", ["store", "partner"])
    .agg(F.avg("discount_pct").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
