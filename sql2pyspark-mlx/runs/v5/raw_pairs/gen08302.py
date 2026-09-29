from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("channel") == "partner").alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.region"))
    .orderBy("region")
)
