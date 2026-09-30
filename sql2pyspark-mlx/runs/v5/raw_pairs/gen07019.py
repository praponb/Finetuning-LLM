from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.filter(F.col("quantity") <= 8).alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_semi")
    .filter(F.col("products.launch_date").between("2024-01-01", "2024-12-07"))
    .select(F.col("products.id"), F.col("products.price"))
    .orderBy(F.col("id").desc())
)
