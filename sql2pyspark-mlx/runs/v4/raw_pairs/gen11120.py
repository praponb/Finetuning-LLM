from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("channel", "order_id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w_running = Window.partitionBy("channel")
w_hist = Window.orderBy(F.col("unit_price_share_minus_amount").desc_nulls_first())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

with_dims = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.amount"), F.col("b.channel"))
)

matched_keys = (
    with_dims
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 8)
        .select("id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("amount_rank", F.rank().over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4))
    .withColumn("unit_price_dense_rank", F.dense_rank().over(w4))
    .withColumn("product_id_sum_amount", F.sum("amount").over(w3))
)

base = (
    enriched
    .filter(F.col("unit_price_dense_rank") != 7)
    .select(
        "order_id",
        "quantity",
        "unit_price",
        "amount",
        "channel",
        "amount_rank",
        "corr_unit_price_quantity",
        "unit_price_share",
        "unit_price_dense_rank",
        "product_id_sum_amount",
        (F.col("unit_price_share") - F.col("amount")).alias("unit_price_share_minus_amount"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

scored = (
    base
    .withColumn("channel_min_unit_price_share_minus_amount", F.min("unit_price_share_minus_amount").over(w_running))
    .withColumn("unit_price_share_minus_amount_rank", F.rank().over(w_hist))
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "quantity",
        "unit_price",
        "corr_unit_price_quantity",
        "unit_price_share",
        "product_id_sum_amount",
        "channel_filled",
        "unit_price_share_minus_amount_rank",
        F.col("min_shipment_cost"),
    )
)
