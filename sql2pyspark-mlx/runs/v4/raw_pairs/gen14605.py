from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("revenue") >= 2700).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.launch_date") > "2024-11-01")
    .select(F.col("p.id"), F.col("p.supplier_id"))
    .orderBy("supplier_id")
)
