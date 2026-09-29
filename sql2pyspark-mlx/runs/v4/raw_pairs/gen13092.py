from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(F.col("p.category"), F.col("p.launch_date"), F.col("t.total_count"))
)
