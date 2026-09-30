from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "BUNDLE"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("n"))
)
