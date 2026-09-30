from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.min("revenue").alias("min_revenue"))
    .filter(F.col("min_revenue") >= 1600)
    .orderBy("store_id", "sale_month")
    .limit(3)
)
