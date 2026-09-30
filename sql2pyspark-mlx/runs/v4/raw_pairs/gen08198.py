from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "WELCOME5"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "channel",
        F.col("VIP_total").alias("vip_total"),
        F.col("VIP_item_count").alias("vip_item_count"),
        F.col("WELCOME5_total").alias("welcome5_total"),
        F.col("WELCOME5_item_count").alias("welcome5_item_count"),
    )
)
