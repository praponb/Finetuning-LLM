from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["VIP", "BUNDLE", "WELCOME5"])
    .agg(F.sum("discount_pct").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "channel",
        F.col("VIP_value_sum").alias("vip_value_sum"),
        F.col("VIP_cnt").alias("vip_cnt"),
        F.col("BUNDLE_value_sum").alias("bundle_value_sum"),
        F.col("BUNDLE_cnt").alias("bundle_cnt"),
        F.col("WELCOME5_value_sum").alias("welcome5_value_sum"),
        F.col("WELCOME5_cnt").alias("welcome5_cnt"),
    )
)
