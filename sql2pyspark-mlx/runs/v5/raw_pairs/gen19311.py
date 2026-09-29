from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 2800)
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)
