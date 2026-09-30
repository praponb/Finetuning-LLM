from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.sale_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.trim(F.col("s.store_name")).alias("clean_store_name"),
    )
    .orderBy("city")
)
