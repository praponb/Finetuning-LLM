from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("store_id").isin([1, 3]) & (F.col("units") <= 26))

summary = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = (
    summary
    .filter(F.col("unique_stores") > 9)
    .orderBy(F.col("store_id").desc())
)
