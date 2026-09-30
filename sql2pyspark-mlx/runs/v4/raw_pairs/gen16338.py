from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

metrics = (
    df_promotions
    .select(
        "*",
        F.min("discount_pct").over(w).alias("promo_code_min_discount_pct"),
        F.ntile(5).over(w2).alias("discount_pct_bucket"),
        F.ntile(5).over(w3).alias("discount_pct_bucket_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_min_discount_pct",
        "discount_pct_bucket",
        "discount_pct_bucket_2",
        F.when(F.col("channel").rlike("^(partner|web)$"), "EXTENDED").otherwise("PRIORITY").alias("channel_zone"),
    )
    .orderBy(F.col("promo_code_min_discount_pct").desc_nulls_last())
)
