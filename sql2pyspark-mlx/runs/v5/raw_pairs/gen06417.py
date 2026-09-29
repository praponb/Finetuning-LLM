from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.product_name"), F.col("p.category"), F.col("ps.last_sale_date"))
    .orderBy(F.col("last_sale_date").desc())
)
