from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft") < 4493).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("ss.sale_date") >= "2024-01-09")
    .select(F.col("ss.id"), F.col("ss.sale_date"))
)
