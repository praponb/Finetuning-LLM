from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.price").between(700, 1020) & F.col("oi.order_id").isin([10, 18, 30, 37]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("p.stock_qty"),
    )
    .orderBy("id")
)
