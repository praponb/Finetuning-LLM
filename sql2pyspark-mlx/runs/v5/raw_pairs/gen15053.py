from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.lower(F.trim(F.col("s.store_name"))) == "downtown express")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
    .orderBy(F.col("id").desc())
)
