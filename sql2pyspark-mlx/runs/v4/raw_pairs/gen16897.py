from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id") == 3).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.city"))
    .orderBy(F.col("id").desc())
)
