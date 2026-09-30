from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("sqft") > 4869).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.status"))
    .orderBy("status")
)
