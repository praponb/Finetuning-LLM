from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("order_date") > "2024-06-01").alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
    )
    .orderBy(F.col("product_id").desc(), F.col("quantity").desc())
)
