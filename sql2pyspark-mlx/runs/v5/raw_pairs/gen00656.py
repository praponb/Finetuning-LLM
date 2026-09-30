from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-09-13", "2024-10-16"))
    .groupBy("product_id")
    .agg(F.avg("units").alias("avg_units"))
    .orderBy(F.col("product_id").desc(), "avg_units")
)
