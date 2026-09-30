from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("revenue").between(2700, 2800)).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(F.col("s.store_name").like("%Old%"))
    .select(F.col("s.id"), F.col("s.sqft"))
)
