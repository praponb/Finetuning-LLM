from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .unionByName(df_products.select(F.col("launch_date").alias("sale_date")))
    .orderBy(F.col("sale_date").desc())
)
