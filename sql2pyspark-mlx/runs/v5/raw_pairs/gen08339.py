from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.col("products.brand"),
        F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"),
    )
    .agg(F.avg(F.col("order_items.unit_price")).alias("avg_unit_price"))
)
