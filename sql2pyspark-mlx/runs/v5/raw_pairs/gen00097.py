from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
    .orderBy(F.col("revenue").desc(), "id")
    .limit(1)
)
