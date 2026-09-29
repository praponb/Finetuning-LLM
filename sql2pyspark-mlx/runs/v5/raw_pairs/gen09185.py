from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("s.sqft") <= 12958)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.concat_ws("/", F.col("o.channel"), F.col("o.status")).alias("channel_status_key"),
    )
)
