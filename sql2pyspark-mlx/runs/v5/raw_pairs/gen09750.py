from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") < 9)
    .orderBy(F.col("units_count").desc(), F.col("sale_month").desc())
)
