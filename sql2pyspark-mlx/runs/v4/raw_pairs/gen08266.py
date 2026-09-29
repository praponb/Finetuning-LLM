from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("product_id").isin([4, 5, 6, 10])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.supplier_id").isin([2, 4, 34]))
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.price"))
)
