from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code")

base = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-07-09").cast("date"))
    .select(
        "*",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    base
    .select(
        "*",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.avg("discount_pct").over(w2).alias("promo_code_avg_discount_pct"),
    )
)
