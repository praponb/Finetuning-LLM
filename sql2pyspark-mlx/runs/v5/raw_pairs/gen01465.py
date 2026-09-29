from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("order_id", "channel").orderBy("prev_quantity_rounded_2", "id")
w4 = Window.partitionBy("order_id").orderBy("unit_price", "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([13, 26, 27, 28]))
    .select("*", F.round("quantity", 0).alias("quantity_rounded"))
)

cleaned_order_items = (
    cleaned
    .filter(F.col("order_id") == 8)
    .select(
        "*",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_band"),
        F.round("quantity", 0).alias("quantity_rounded_2"),
    )
)

scored = (
    cleaned_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity_rounded",
        "quantity_band",
        F.lead("quantity_rounded_2").over(w_running).alias("next_quantity_rounded_2"),
        F.lag("quantity_rounded_2").over(w_running).alias("prev_quantity_rounded_2"),
        F.lead("quantity_rounded").over(w_running).alias("next_quantity_rounded"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.unit_price",
        "a.quantity_band",
        "a.prev_quantity_rounded_2",
        "a.next_quantity_rounded",
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
)

metrics = (
    joined
    .filter(F.col("channel") != "store")
    .select(
        "*",
        F.count("unit_price").over(w2).alias("channel_count_unit_price"),
        F.ntile(5).over(w3).alias("prev_quantity_rounded_2_bucket"),
        F.ntile(10).over(w4).alias("unit_price_bucket"),
    )
)

result = (
    metrics
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity_band",
        "prev_quantity_rounded_2",
        "shipping_fee",
        "channel_count_unit_price",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("prev_quantity_rounded_2").desc_nulls_first())
)
