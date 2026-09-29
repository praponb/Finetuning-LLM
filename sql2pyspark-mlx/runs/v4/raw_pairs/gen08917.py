from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3000)
    .groupBy("product_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") < 49)
    .orderBy("avg_units")
)
