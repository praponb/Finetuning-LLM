from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-05-23", "2024-07-01"))
    .agg(F.count("*").alias("store_sale_count"))
    .orderBy(F.col("store_sale_count").desc())
)
