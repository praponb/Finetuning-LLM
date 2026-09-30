from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([5, 6, 10]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.channel"),
        F.col("p.category"),
        F.col("p.cost"),
    )
    .orderBy("channel")
)
