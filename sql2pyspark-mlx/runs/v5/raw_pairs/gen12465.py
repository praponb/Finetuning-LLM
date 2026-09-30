from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.orderBy("discount_pct")
w3 = Window.orderBy("starts_on", "id")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w_ordered = Window.orderBy(F.col("discount_pct_dense_rank_2").desc(), "id")
w_hist = Window.orderBy("prev_discount_pct", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.orderBy("prev_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cumulative_discount_pct", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("channel_count_discount_pct", "id")
w11 = Window.orderBy("discount_pct_dense_rank_avg_2", "id")
w12 = Window.orderBy(F.col("running_discount_pct_dense_rank").desc_nulls_first())
w13 = Window.orderBy("discount_pct_dense_rank")
w14 = Window.orderBy(F.col("prev_discount_pct").desc(), "id")

enriched = (
    df_promotions
    .filter(F.col("discount_pct") > 0.27)
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.count("discount_pct").over(w).alias("channel_count_discount_pct"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.lag("discount_pct", 1, 0).over(w3).alias("prev_discount_pct"),
        F.sum("discount_pct").over(w4).alias("cumulative_discount_pct"),
        F.dense_rank().over(w5).alias("discount_pct_dense_rank_2"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.avg("channel_count_discount_pct").over(w_hist).alias("channel_count_discount_pct_avg_2"),
        F.avg("discount_pct_dense_rank").over(w_hist).alias("discount_pct_dense_rank_avg_2"),
    )
)

metrics_promotions = (
    metrics
    .select(
        "*",
        F.sum("discount_pct_dense_rank").over(w8).alias("running_discount_pct_dense_rank"),
        F.sum("channel_count_discount_pct").over(w8).alias("cumulative_channel_count_discount_pct"),
        F.avg("channel_count_discount_pct_avg_2").over(w9).alias("channel_count_discount_pct_avg_2_avg_5"),
        F.lead("channel_count_discount_pct", 2).over(w10).alias("next_channel_count_discount_pct"),
    )
)

metrics_promotions_2 = (
    metrics_promotions
    .select(
        "id",
        "ends_on",
        "discount_pct_dense_rank",
        "cumulative_discount_pct",
        "discount_pct_dense_rank_2",
        "channel_count_discount_pct_avg_2",
        "channel_count_discount_pct_avg_2_avg_5",
        "next_channel_count_discount_pct",
        F.row_number().over(w11).alias("event_seq_2"),
        F.cume_dist().over(w12).alias("running_discount_pct_dense_rank_cume_dist"),
        F.dense_rank().over(w13).alias("discount_pct_dense_rank_dense_rank"),
        F.ntile(3).over(w14).alias("prev_discount_pct_bucket"),
    )
)

result = (
    metrics_promotions_2
    .select(
        "id",
        "ends_on",
        "discount_pct_dense_rank",
        "cumulative_discount_pct",
        "discount_pct_dense_rank_2",
        "channel_count_discount_pct_avg_2",
        "channel_count_discount_pct_avg_2_avg_5",
        "next_channel_count_discount_pct",
        "event_seq_2",
    )
    .orderBy(F.col("next_channel_count_discount_pct").desc_nulls_first(), "ends_on")
)
