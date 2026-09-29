from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.filter(F.col("revenue").between(1800, 2000)).alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_anti")
    .filter(F.col("stores.city").isin(["Berlin", "London"]))
    .select(F.col("stores.id"), F.col("stores.city"), F.col("stores.sqft"))
)
