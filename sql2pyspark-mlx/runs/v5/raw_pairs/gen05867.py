from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") >= 1300)
    .orderBy("sale_year")
)
