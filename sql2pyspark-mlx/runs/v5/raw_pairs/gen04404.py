from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_anti")
    .filter(F.col("stores.store_name").like("O%"))
    .select(F.col("stores.id"), F.col("stores.sqft"))
    .orderBy(F.col("id").desc())
)
