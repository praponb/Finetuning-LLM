from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 3400) & (F.col("sale_date") < "2024-10-01"))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy(F.col("sale_quarter").desc())
    .limit(1)
)
