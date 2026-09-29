from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("week", "ends_on").alias("week_start"))
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
    )
    .filter(F.col("discount_pct_per_vip") > 0)
)
