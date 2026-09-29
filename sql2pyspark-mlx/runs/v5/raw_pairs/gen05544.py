from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]))
    .select("units")
    .unionByName(df_store_sales.filter(F.col("product_id") == 3).select("units"))
    .orderBy(F.col("units").desc())
)
