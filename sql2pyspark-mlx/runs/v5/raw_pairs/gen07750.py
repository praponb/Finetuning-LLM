from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") < 500)
    .select(
        F.col("p.category"),
        F.col("p.price"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
    )
    .orderBy(F.col("price").desc())
)
