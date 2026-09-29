from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("o.store_id").isNotNull() | (F.col("p.launch_date") > "2025-06-01"))
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
    .orderBy("amount")
)
