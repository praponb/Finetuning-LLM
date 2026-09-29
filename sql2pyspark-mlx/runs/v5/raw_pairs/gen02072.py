from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 6, 12]))
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(F.col("p.id"), F.col("p.product_name"), F.col("P.min_sale_date"))
)
