from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .orderBy("revenue_range")
)
