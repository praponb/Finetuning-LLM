from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.product_name"))
    .orderBy(F.col("price").desc(), F.col("product_name").desc(), "id")
    .limit(5)
)
