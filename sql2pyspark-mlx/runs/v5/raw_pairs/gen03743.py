from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.max("discount_pct").over(w).alias("last4_max_discount_pct"),
        F.sum("discount_pct").over(w).alias("last4_sum_discount_pct"),
        F.min("discount_pct").over(w).alias("last4_min_discount_pct"),
        F.avg("discount_pct").over(w).alias("last4_avg_discount_pct"),
        F.rank().over(w2).alias("discount_pct_rank"),
    )
)
