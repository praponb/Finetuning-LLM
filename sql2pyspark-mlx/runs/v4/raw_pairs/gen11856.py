from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("city").isin(["Singapore", "Berlin"])).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.channel").isin(["store", "web"]))
    .select(F.col("o.id"), F.col("o.store_id"))
)
