from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("starts_on", "id")

metrics = (
    df_promotions
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.lag("discount_pct", 1).over(w2).alias("prev_discount_pct"),
    )
)

result = (
    metrics
    .groupBy("promo_code")
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "VIP", 1)), F.lit(0)), 2).alias("discount_pct_per_vip"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
    )
    .orderBy(F.col("partner_count").desc())
)
