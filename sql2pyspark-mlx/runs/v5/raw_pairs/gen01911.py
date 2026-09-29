from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "BUNDLE", "WELCOME5"])
    .agg(F.sum("discount_pct"))
    .select(
        "channel",
        F.col("VIP").alias("vip"),
        F.col("BUNDLE").alias("bundle"),
        F.col("WELCOME5").alias("welcome5"),
    )
)
