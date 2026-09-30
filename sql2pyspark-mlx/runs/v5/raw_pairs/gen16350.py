from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w_grp = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel", "promo_code")
w_running = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-05-11").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
    )
)

result = (
    filtered
    .filter(F.col("channel").isin(["store", "partner", "mobile"]))
    .select(
        "id",
        "starts_on",
        F.lag("discount_pct", 1, 0).over(w).alias("prev_discount_pct"),
        F.ntile(10).over(w_grp).alias("discount_pct_bucket"),
        F.count("discount_pct").over(w3).alias("channel_count_discount_pct"),
        F.ntile(3).over(w_running).alias("discount_pct_bucket_2"),
        F.ntile(4).over(w_grp).alias("discount_pct_bucket_3"),
    )
)
