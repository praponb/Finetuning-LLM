from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-5, -1)
w_hist = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "*",
        F.ntile(3).over(w).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5_prior"),
        F.first("channel", ignorenulls=True).over(w_hist).alias("first_known_channel"),
    )
)
