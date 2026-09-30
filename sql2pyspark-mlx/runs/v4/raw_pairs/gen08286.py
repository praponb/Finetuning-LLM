from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") <= 213)
    .select(F.col("ss.units"), F.col("p.category"))
    .orderBy("category")
)
