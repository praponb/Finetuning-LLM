from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id")

scored = (
    df_promotions
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w2))
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.date_trunc("WEEK", "ends_on").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.max(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct"))).alias("bundle_max_discount_pct"),
        F.round(F.sum("next_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("next_discount_pct_per_welcome5"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
)
