from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.num_store_sales") >= 5)
    .select(F.col("p.category"), F.col("p.supplier_id"), F.col("pt.num_store_sales"))
)
