from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "left")
    .filter(F.col("product_stats.max_units") >= 26)
    .select(F.col("products.stock_qty"), F.col("product_stats.max_units"))
    .orderBy(F.col("stock_qty").desc())
)
