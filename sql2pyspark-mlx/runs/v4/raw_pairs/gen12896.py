from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-11-01")
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.launch_date"), F.col("p.price"), F.col("t.num_store_sales"))
)
