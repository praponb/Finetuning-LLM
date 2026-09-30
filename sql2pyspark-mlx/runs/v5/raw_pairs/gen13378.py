from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id") == 7).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.store_name"))
    .orderBy(F.col("store_name").desc(), "id")
    .limit(20)
)
