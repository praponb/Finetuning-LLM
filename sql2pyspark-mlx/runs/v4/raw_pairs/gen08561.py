from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("discount_pct_whole", F.floor("discount_pct"))
)

result = (
    cleaned
    .groupBy("promo_code")
    .pivot("channel", ["web", "partner"])
    .agg(F.avg("discount_pct_whole").alias("value_sum"))
)
