from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull() & (F.year("sale_date") == 2024) & (F.col("units") <= 38))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)
