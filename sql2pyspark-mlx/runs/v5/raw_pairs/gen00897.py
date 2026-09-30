from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id")
w2 = Window.partitionBy("channel")
w_ordered = Window.orderBy(F.col("discount_pct").desc(), "id")

enriched = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.lead("discount_pct", 2).over(w).alias("next_discount_pct"),
        F.max("discount_pct").over(w2).alias("channel_max_discount_pct"),
        F.ntile(5).over(w_ordered).alias("discount_pct_bucket"),
        F.ntile(3).over(w_ordered).alias("discount_pct_bucket_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "discount_pct",
        "next_discount_pct",
        "channel_max_discount_pct",
        "discount_pct_bucket_2",
    )
)
