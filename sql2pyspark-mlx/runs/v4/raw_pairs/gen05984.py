from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")

with_windows = (
    df_promotions
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5"),
        F.lead("discount_pct", 1).over(w3).alias("next_discount_pct"),
    )
)

result = (
    with_windows
    .groupBy(
        "promo_code",
        F.when(F.col("discount_pct") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount_pct_avg_5")).otherwise(0)).alias("mobile_discount_pct_avg_5"),
    )
)
