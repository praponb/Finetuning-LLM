from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.unit_price") < 160)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.cost"),
    )
)
