from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("units") != 32).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.price").between(890, 1130))
    .select(F.col("p.id"), F.col("p.supplier_id"))
    .orderBy("id")
)
