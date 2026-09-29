from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .filter(F.col("product_id").isin([5, 8, 9]))
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.col("product_totals.unique_products"),
    )
    .orderBy("unique_products")
)
