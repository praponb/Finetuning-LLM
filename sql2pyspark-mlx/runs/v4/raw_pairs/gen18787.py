from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_order_items.filter(F.col("order_id").isin([10, 32])).alias("order_items"), F.col("products.id") == F.col("order_items.product_id"), "left_semi")
    .filter(F.col("products.supplier_id").isin([9, 38, 40]))
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.product_name"),
    )
    .orderBy("id")
)
