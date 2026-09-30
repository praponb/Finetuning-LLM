from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on_plus_85d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w_running = Window.orderBy("starts_on_plus_85d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel", "promo_code").orderBy(F.col("prev_discount_pct").desc_nulls_first())
w6 = Window.orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code", "channel")
w9 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w10 = Window.partitionBy("channel", "promo_code").orderBy("promo_code_sum_prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("channel").orderBy("prev_discount_pct_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("promo_code").orderBy("promo_code_sum_prev_discount_pct", "id")
w13 = Window.partitionBy("promo_code")
w14 = Window.partitionBy("channel").orderBy(F.col("prev_discount_pct").desc(), "id")
w15 = Window.partitionBy("channel").orderBy("prev_discount_pct", "id")
w16 = Window.partitionBy("channel").orderBy("cumulative_discount_pct_percent_rank_2", "id")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-10-14"))) & (F.col("ends_on") < "2023-06-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        F.date_add("starts_on", 85).alias("starts_on_plus_85d"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.cume_dist().over(w_ordered).alias("discount_pct_cume_dist"),
        F.percent_rank().over(w_ordered).alias("discount_pct_percent_rank"),
        F.lag("discount_pct", 1).over(w3).alias("prev_discount_pct"),
        F.first("channel", ignorenulls=True).over(w_running).alias("first_known_channel_2"),
    )
)

scored = (
    enriched
    .withColumn("prev_discount_pct_dense_rank", F.dense_rank().over(w5))
    .withColumn("cumulative_discount_pct_percent_rank", F.sum("discount_pct_percent_rank").over(w6))
    .withColumn("cumulative_discount_pct_percent_rank_2", F.sum("discount_pct_percent_rank").over(w7))
    .withColumn("promo_code_sum_prev_discount_pct", F.sum("prev_discount_pct").over(w8))
    .withColumn("prev_discount_pct_avg_2_prior", F.avg("prev_discount_pct").over(w9))
)

scored_promotions = (
    scored
    .withColumn("running_prev_discount_pct_dense_rank", F.sum("prev_discount_pct_dense_rank").over(w10))
    .withColumn("first_known_prev_discount_pct_avg_2_prior", F.first("prev_discount_pct_avg_2_prior", ignorenulls=True).over(w11))
)

result = (
    scored_promotions
    .withColumn("promo_code_sum_prev_discount_pct_bucket", F.ntile(3).over(w12))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w13)), 4))
    .withColumn("event_seq", F.row_number().over(w14))
    .withColumn("seq_no", F.row_number().over(w15))
    .withColumn("cumulative_discount_pct_percent_rank_2_bucket", F.ntile(4).over(w16))
)
