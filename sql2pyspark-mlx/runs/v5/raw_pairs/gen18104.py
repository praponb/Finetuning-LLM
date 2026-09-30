from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .select(
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
)
