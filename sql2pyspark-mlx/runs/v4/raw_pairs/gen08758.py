from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("discount") <= 0.39).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.shipped_date") < "2023-05-08")
    .select(F.col("s.id"), F.col("s.carrier"), F.col("s.cost"), F.col("s.order_id"))
)
