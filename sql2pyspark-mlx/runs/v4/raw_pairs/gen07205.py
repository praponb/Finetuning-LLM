from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 5)
    .orderBy("product_name")
)
