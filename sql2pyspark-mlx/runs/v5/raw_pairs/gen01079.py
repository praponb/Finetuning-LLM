from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("revenue").between(2000, 3900)).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.product_name").isNotNull())
    .select(F.col("p.id"), F.col("p.cost"))
)
