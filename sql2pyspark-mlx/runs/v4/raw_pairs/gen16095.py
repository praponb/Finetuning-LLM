from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .select(
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("product_totals.total_quantity"),
        F.col("product_totals.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "total_quantity")
)
