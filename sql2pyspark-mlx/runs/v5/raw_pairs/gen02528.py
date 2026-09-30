from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_semi")
    .filter(F.col("stores.sqft") > 9799)
    .select(F.col("stores.id"), F.col("stores.opened_date"), F.col("stores.city"))
)
