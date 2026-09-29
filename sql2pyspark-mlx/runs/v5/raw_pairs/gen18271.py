from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-01", "2023-07-01"))
    .agg(F.max("units").alias("max_units"))
    .orderBy("max_units")
)
