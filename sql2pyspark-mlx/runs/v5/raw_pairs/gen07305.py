from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .filter(F.col("ssbs.total_units") > 200)
    .select(F.col("s.region"), F.col("s.sqft"), F.col("ssbs.total_units"))
    .orderBy("total_units", F.col("region").desc())
)
