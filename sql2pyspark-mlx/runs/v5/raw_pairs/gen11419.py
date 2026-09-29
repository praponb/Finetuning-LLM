from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2023-05-31")
    .groupBy("store_id")
    .agg(F.max("revenue").alias("max_revenue"))
)
