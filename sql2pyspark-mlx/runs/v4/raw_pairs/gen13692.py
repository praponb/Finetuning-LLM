from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units") <= 26)

store_sales_stats = (
    base
    .groupBy("store_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    store_sales_stats
    .filter(F.col("avg_revenue") < 79)
    .orderBy(F.col("avg_revenue").desc())
)
