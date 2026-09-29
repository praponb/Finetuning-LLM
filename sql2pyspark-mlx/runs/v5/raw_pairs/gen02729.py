from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("ss.sale_date")).alias("min_sale_date"))
    .orderBy("brand")
)
