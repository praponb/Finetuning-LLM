from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price_whole").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price_whole", "id")
w3 = Window.partitionBy("order_id").orderBy("quantity")
w4 = Window.partitionBy("order_id").orderBy("event_seq", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

valid = (
    df_order_items
    .filter(F.col("product_id").isin([7, 32, 36, 39]))
    .select(
        "id",
        "order_id",
        "quantity",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_whole",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w2).alias("unit_price_whole_bucket"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.discount"))
)

enriched = (
    joined
    .withColumn("quantity_dense_rank", F.dense_rank().over(w3))
    .withColumn("prev_discount", F.lag("discount", 1).over(w4))
)

base = (
    enriched
    .filter(F.col("unit_price_whole") <= 31)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_whole",
        "event_seq",
        "unit_price_whole_bucket",
        "discount",
        "quantity_dense_rank",
        F.round((F.col("prev_discount") * 0.85), 2).alias("prev_discount_adjusted"),
        F.ceil("discount").alias("discount_whole"),
    )
)

result = (
    base
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price_whole",
        "event_seq",
        "unit_price_whole_bucket",
        "quantity_dense_rank",
        "discount_whole",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("quantity").desc())
)
