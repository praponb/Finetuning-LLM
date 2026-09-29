from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.sqft") < 12032)
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("stores.store_name"),
        F.col("stores.city"),
        F.col("stores.sqft"),
    )
    .orderBy("store_name")
)
