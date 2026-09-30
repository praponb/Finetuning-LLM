from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("order_date") > "2023-07-06").alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.region").isin(["Central", "South"]))
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.sqft"))
    .orderBy(F.col("id").desc())
)
