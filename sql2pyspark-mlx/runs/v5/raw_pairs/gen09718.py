from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id").isin([6, 10])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .filter(F.col("s.region").isin(["West", "East"]))
    .select(F.col("s.id"), F.col("s.city"), F.col("s.store_name"), F.col("s.region"))
    .orderBy("id")
)
