from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy(F.col("shipping_fee").asc_nulls_first())
w4 = Window.orderBy("shipping_fee")
w5 = Window.partitionBy("order_id").orderBy("shipping_fee_percent_rank")
w6 = Window.orderBy("amount")

ranked = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([16, 27]))
        .select("order_id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        F.col("b.amount"),
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
    )
)

enriched = (
    joined
    .withColumn("order_id_sum_amount", F.sum("amount").over(w2))
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w3))
    .withColumn("shipping_fee_percent_rank", F.percent_rank().over(w4))
)

matched_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_shipments
        .filter(F.col("cost") < 44)
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id").eqNullSafe(F.col("o.id")), "left")
    .select("e.*", F.col("o.channel"), F.col("o.discount"))
)

scored = (
    with_dims
    .select(
        "order_id",
        "delivered_date",
        "amount",
        "shipping_fee",
        "shipping_fee_cume_dist",
        "discount",
        F.dense_rank().over(w5).alias("shipping_fee_percent_rank_dense_rank"),
        F.dense_rank().over(w6).alias("amount_dense_rank"),
    )
)

summary = (
    scored
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("activity_month"))
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("amount_dense_rank", 0.5).alias("amount_dense_rank_p50"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)

result = (
    summary
    .select("activity_month", "amount_dense_rank_p50", "num_rows")
    .orderBy(F.col("activity_month").asc_nulls_last())
)
