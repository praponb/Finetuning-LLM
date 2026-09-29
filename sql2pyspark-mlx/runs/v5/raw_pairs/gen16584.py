from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "left")
    .filter(F.col("product_totals.total_units") >= 200)
    .select(F.col("products.launch_date"), F.col("product_totals.total_units"))
)
