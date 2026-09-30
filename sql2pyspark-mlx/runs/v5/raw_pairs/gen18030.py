from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("product_totals.quantity_range"),
    )
    .orderBy(F.col("category").desc(), F.col("id").desc())
)
