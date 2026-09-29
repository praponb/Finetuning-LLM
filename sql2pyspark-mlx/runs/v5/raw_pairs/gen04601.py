from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_channel").orderBy("shipping_fee", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

filtered = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.status"))
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isNull())
    .select("a.id", "a.order_id", F.col("d.discount"), F.col("d.channel"))
)

joined_order_items = (
    joined
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        F.col("p.shipping_fee"),
        F.col("p.channel").alias("order_channel"),
        F.col("p.status"),
    )
)

with_windows = (
    joined_order_items
    .select(
        "id",
        "order_id",
        "shipping_fee",
        "order_channel",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipping_fee",
        "first_known_status",
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("shipping_fee").desc_nulls_first())
)
