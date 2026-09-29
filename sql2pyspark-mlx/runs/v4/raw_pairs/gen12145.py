from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("product_id").isin([1, 3, 4, 8])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.store_name"))
    .orderBy(F.col("store_name").desc())
)
