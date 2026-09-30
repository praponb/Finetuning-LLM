from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.shipping_fee") >= 39)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.last_day(F.col("p.launch_date")).alias("launch_date_month_end"),
    )
)
