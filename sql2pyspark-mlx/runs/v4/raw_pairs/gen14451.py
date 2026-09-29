from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code")
w3 = Window.partitionBy("channel")
w4 = Window.orderBy("discount_pct_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("promo_code_channel_key", F.concat_ws("-", "promo_code", "channel"))
    .withColumn("promo_code_channel_key_2", F.concat_ws("-", "promo_code", "channel"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w).alias("discount_pct_dense_rank"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

enriched = (
    metrics
    .withColumn("discount_pct_dense_rank_share", F.round((F.col("discount_pct_dense_rank") / F.sum("discount_pct_dense_rank").over(w3)), 4))
    .withColumn("last_known_discount_pct_dense_rank", F.last("discount_pct_dense_rank", ignorenulls=True).over(w4))
)

result = (
    enriched
    .select(
        "*",
        F.stddev_samp("last_known_discount_pct_dense_rank").over(w2).alias("promo_code_stddev_samp_last_known_discount_pct_dense_rank"),
        F.sum("discount_pct_dense_rank_share").over(w5).alias("cumulative_discount_pct_dense_rank_share"),
    )
)
