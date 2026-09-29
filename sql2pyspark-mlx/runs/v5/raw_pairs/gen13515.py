from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.year("opened_date") >= 2023).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.revenue").between(1200, 2800))
    .select(F.col("ss.id"), F.col("ss.units"), F.col("ss.sale_date"))
)
