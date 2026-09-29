from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-03-19")
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.unique_stores") >= 5)
    .select(F.col("products.stock_qty"), F.col("t.unique_stores"))
    .orderBy("stock_qty")
)
