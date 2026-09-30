from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity_filled", "id")
w2 = Window.orderBy(F.col("quantity_filled").desc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([20, 24, 38]))
    .select(
        "id",
        "order_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lag("quantity_filled").over(w).alias("prev_quantity_filled"),
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("quantity_filled_dense_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("prev_quantity_filled") < 49)
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_filled",
        "next_unit_price",
        "prev_unit_price",
        "prev_quantity_filled",
        F.round((F.col("row_seq") / F.col("unit_price")), 2).alias("row_seq_per_unit_price"),
        F.ceil("quantity_filled_dense_rank").alias("quantity_filled_dense_rank_whole"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.channel"), F.col("d.discount"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.amount"))
)

result = (
    joined
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity_filled",
        "next_unit_price",
        "latest_rank",
        "channel",
        "amount",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("quantity_filled").asc_nulls_last())
)
