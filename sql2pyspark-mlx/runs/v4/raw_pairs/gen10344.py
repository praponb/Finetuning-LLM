from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.filter(F.year("sale_date") >= 2025).alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_anti")
    .select(F.col("stores.id"), F.col("stores.city"))
    .orderBy("city")
)
