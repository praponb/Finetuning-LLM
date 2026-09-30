from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.upper(F.col("s.store_name")).like("%OWN%") | (F.col("ss.revenue") >= 3700))
    .select(F.col("s.sqft"), F.col("ss.units"))
)
