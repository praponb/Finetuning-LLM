from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .pivot("channel", ["mobile", "partner"])
    .agg(F.avg("discount_pct").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
