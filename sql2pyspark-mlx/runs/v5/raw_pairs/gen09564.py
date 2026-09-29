from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("starts_on", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")

result = (
    df_promotions
    .filter(F.col("discount_pct") <= 0.28)
    .select(
        "id",
        "channel",
        F.avg("discount_pct").over(w_recent).alias("discount_pct_avg_3_prior"),
        F.percent_rank().over(w2).alias("discount_pct_percent_rank"),
    )
)
