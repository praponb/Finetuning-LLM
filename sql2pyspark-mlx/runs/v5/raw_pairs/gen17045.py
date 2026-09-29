from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel", "order_id").orderBy("quantity_whole", "id")
w2 = Window.partitionBy("order_id").orderBy("next_unit_price")
w3 = Window.partitionBy("channel", "order_id").orderBy(F.col("prev_unit_price").desc_nulls_first(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_whole", F.ceil("quantity"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([4, 38]))
    .select("s.*", F.col("o.order_date"), F.col("o.channel"))
)

scored = (
    with_dims
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "quantity_whole",
        "channel",
        F.lead("quantity_whole").over(w_ordered).alias("next_quantity_whole"),
        F.lag("unit_price").over(w_ordered).alias("prev_unit_price"),
        F.lead("unit_price").over(w_ordered).alias("next_unit_price"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.channel",
        "s.next_quantity_whole",
        "s.prev_unit_price",
        "s.next_unit_price",
        "s.row_seq",
        F.col("d.amount"),
        F.col("d.status"),
    )
)

valid_order_items = (
    joined
    .filter(F.col("row_seq").between(5, 6))
    .select(
        "id",
        "order_id",
        "channel",
        "prev_unit_price",
        "next_unit_price",
        "row_seq",
        "amount",
        F.round((F.col("next_quantity_whole") * 1.1), 2).alias("next_quantity_whole_scaled"),
        F.col("next_unit_price").cast("int").alias("next_unit_price_int"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

ranked_order_items = (
    valid_order_items
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_order_items
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "channel",
        "prev_unit_price",
        "next_quantity_whole_scaled",
        "next_unit_price_int",
        "status_filled",
        "latest_rank",
        F.col("sum_order_item_quantity"),
    )
)
