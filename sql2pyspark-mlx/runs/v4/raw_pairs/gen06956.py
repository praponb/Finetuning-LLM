from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_anti")
    .filter(~F.col("s.city").isin(["Singapore", "Tokyo", "New York"]))
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.region"))
)
