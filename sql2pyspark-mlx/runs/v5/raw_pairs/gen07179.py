from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("shipping_fee") >= 14)
    .groupBy("store_id")
    .agg(F.countDistinct("discount").alias("unique_statuss"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.unique_statuss") > 3)
    .select(F.col("s.region"), F.col("s.sqft"), F.col("obs.unique_statuss"))
    .orderBy("unique_statuss")
)
