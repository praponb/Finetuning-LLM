from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.orderBy("ends_on", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w5 = Window.orderBy("discount_pct")

base = (
    df_promotions
    .filter(F.col("ends_on") > "2023-11-01")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.date_trunc("WEEK", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "channel_filled",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2"),
        F.ntile(3).over(w4).alias("discount_pct_bucket"),
        F.cume_dist().over(w5).alias("discount_pct_cume_dist"),
    )
)
