from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("method")
w5 = Window.orderBy("amount_share")
w6 = Window.partitionBy("channel").orderBy(F.col("amount_share").asc_nulls_last())
w_part = Window.partitionBy("channel")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

remaining_keys = (
    df_payments
    .select("status", "id")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isin([5, 17, 20, 27]))
        .select("status", "id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.avg("amount").over(w).alias("amount_avg_7d"),
        F.rank().over(w2).alias("amount_rank"),
        F.rank().over(w3).alias("amount_rank_2"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share_2"),
    )
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.id", "src.order_id", "src.amount_share", F.col("p.channel"))
)

scored = (
    enriched_payments
    .filter(F.col("channel") != "partner")
    .select(
        "order_id",
        "amount_share",
        "channel",
        F.rank().over(w5).alias("amount_share_rank"),
        F.rank().over(w6).alias("amount_share_rank_2"),
        F.max("amount_share").over(w_part).alias("channel_max_amount_share"),
        F.stddev_samp("amount_share").over(w_part).alias("channel_stddev_samp_amount_share"),
    )
)

summary = (
    scored
    .groupBy(
        F.when(F.col("amount_share_rank_2") >= 6, "LARGE").otherwise("SMALL").alias("amount_share_rank_2_tier"),
        "order_id",
        F.when(F.col("amount_share_rank") >= 8, "LARGE").otherwise("SMALL").alias("amount_share_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount_share_rank_2"))).alias("mobile_total_amount_share_rank_2"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.sum("channel_stddev_samp_amount_share").alias("total_channel_stddev_samp_amount_share"),
        F.percentile_approx("amount_share", [0.25, 0.5, 0.75]).alias("amount_share_quartiles"),
    )
)

result = (
    summary
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "amount_share_rank_2_tier",
        "order_id",
        "amount_share_rank_tier",
        "mobile_total_amount_share_rank_2",
        "store_count",
        "amount_share_quartiles",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(F.col("avg_order_item_quantity").asc_nulls_last())
)
