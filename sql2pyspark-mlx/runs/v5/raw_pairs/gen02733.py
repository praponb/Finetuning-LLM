from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("product_id") == 3)

agg = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    agg
    .filter(F.col("min_units") >= 13)
    .orderBy(F.col("min_units").desc())
)
