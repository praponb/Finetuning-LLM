from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
    )
)
