from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.stock_qty"), F.col("ps.first_sale_date"))
)
