from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.city").isin(["Berlin", "Tokyo", "New York"]))
    .select(F.col("s.id"), F.col("s.region"), F.col("s.sqft"), F.col("s.store_name"))
)
