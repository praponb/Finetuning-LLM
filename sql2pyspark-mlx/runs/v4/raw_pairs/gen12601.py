from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 3]))
    .select("units")
    .intersect(df_store_sales.filter(F.col("store_id").isin([4, 6])).select("units"))
    .orderBy(F.col("units").desc())
)
