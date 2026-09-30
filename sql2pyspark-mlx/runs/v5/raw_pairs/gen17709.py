from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue") < 2200)
    .select(
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
