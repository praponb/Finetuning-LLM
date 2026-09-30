from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(28, 32))
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") > 65)
    .orderBy(F.col("revenue_range").desc())
)
