from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.month("sale_date").alias("sale_month"))
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") >= 64)
)
