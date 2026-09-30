from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.month("opened_date").isin([5, 7, 12])).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("ss.revenue") >= 2100)
    .select(F.col("ss.id"), F.col("ss.revenue"))
)
