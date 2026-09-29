from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.id"), F.col("p.category"), F.col("t.last_sale_date"))
)
