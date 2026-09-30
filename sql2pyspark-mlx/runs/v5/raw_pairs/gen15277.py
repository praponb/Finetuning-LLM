from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-08-03").cast("date")) & (F.col("discount_pct") >= 0.2))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_sub("starts_on", 76).alias("starts_on_minus_76d"),
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
    )
)

result = (
    base
    .groupBy("channel")
    .pivot("promo_code", ["WELCOME5", "VIP"])
    .agg(
        F.avg("discount_pct_rounded").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "channel",
        F.col("WELCOME5_value_sum").alias("welcome5_value_sum"),
        F.col("WELCOME5_cnt").alias("welcome5_cnt"),
        F.col("VIP_value_sum").alias("vip_value_sum"),
        F.col("VIP_cnt").alias("vip_cnt"),
    )
)
