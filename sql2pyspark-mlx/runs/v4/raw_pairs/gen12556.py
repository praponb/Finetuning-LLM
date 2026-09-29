from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
    )
    .orderBy("revenue")
)
