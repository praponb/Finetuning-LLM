from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w5 = Window.orderBy("discount_pct")
w6 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w7 = Window.partitionBy("promo_code", "channel").orderBy("latest_rank", "id")
w8 = Window.partitionBy("channel")
w9 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("discount_pct").desc(), "id")

ranked = (
    df_promotions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked_promotions = (
    ranked
    .withColumn("latest_rank_2", F.row_number().over(w2))
    .filter(F.col("latest_rank_2") == 1)
)

metrics = (
    ranked_promotions
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w3))
    .withColumn("discount_pct_bucket", F.ntile(3).over(w4))
    .withColumn("discount_pct_rank", F.rank().over(w5))
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w6))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2, 0).over(w7))
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

metrics_promotions = (
    deduped
    .select(
        "*",
        F.count("prev_discount_pct").over(w8).alias("channel_count_prev_discount_pct"),
        F.sum("discount_pct").over(w9).alias("running_discount_pct"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    metrics_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "latest_rank_2",
        "discount_pct_rank",
        "discount_pct_dense_rank",
        "prev_discount_pct",
        "row_num",
        "channel_count_prev_discount_pct",
    )
)
