from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("channel").orderBy("seq_no", "id")
w3 = Window.orderBy(F.col("prev_shipping_fee").desc_nulls_last())
w4 = Window.orderBy("prev_discount")

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity").isNotNull()), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "channel",
        F.lag("shipping_fee").over(w_running).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w_running).alias("next_shipping_fee"),
        F.lag("discount").over(w_running).alias("prev_discount"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "prev_shipping_fee", "prev_discount")
)

matched_keys = (
    latest
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("amount").isNull())
        .select("id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "prev_shipping_fee",
        "prev_discount",
        F.rank().over(w3).alias("prev_shipping_fee_rank"),
        F.dense_rank().over(w4).alias("prev_discount_dense_rank"),
        F.rank().over(w4).alias("prev_discount_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("prev_shipping_fee_rank") >= 8, "HIGH").otherwise("LOW").alias("prev_shipping_fee_rank_tier"),
    )
    .agg(
        F.round(F.avg("prev_discount_dense_rank"), 2).alias("avg_prev_discount_dense_rank"),
        F.percentile_approx("prev_discount", [0.25, 0.5, 0.75]).alias("prev_discount_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_prev_discount_dense_rank") > 0)
    .orderBy("distinct_ids")
)
