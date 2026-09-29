from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())

enriched = (
    df_promotions
    .select(
        "*",
        F.ntile(5).over(w).alias("discount_pct_bucket"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_pct_bucket") >= 8, "LARGE").otherwise("SMALL").alias("discount_pct_bucket_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
    )
    .orderBy(F.col("discount_pct_bucket_tier").asc_nulls_first())
)
