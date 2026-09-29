from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") >= 80)
    .orderBy(F.col("store_id").desc())
)
