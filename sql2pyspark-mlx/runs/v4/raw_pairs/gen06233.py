from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select("units")
    .intersect(df_store_sales.filter(F.col("units") == 39).select("units"))
    .orderBy("units")
)
