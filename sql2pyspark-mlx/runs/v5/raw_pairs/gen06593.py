from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("discount") <= 0.23).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.city").isin(["Singapore", "Chiang Mai"]))
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.sqft"), F.col("s.city"))
)
