from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .filter(F.col("O.stddev_amount") > 51)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("O.stddev_amount"))
    .orderBy(F.col("stddev_amount").desc(), F.col("id").desc())
)
