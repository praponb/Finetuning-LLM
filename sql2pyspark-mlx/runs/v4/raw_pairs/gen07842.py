from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-07-01", "2024-04-01"))
    .groupBy("store_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)
