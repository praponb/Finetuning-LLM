from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-5, -1)

enriched = (
    df_promotions
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_6"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "discount_pct_avg_6",
        "discount_pct_avg_5_prior",
    )
    .orderBy(F.col("discount_pct_avg_5_prior").desc())
)
