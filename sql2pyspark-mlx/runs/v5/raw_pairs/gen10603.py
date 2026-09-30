from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id").isin([5, 9])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .filter(F.col("s.city") == "New York")
    .select(F.col("s.id"), F.col("s.city"))
    .orderBy("city")
)
