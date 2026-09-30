from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id")

with_windows = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.sum("discount_pct").over(w_recent).alias("last7_sum_discount_pct"),
        F.max("discount_pct").over(w_recent).alias("last7_max_discount_pct"),
        F.avg("discount_pct").over(w_recent).alias("last7_avg_discount_pct"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "last7_sum_discount_pct",
        "last7_max_discount_pct",
        "last7_avg_discount_pct",
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
