from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.discount") > 0.33) | (F.col("o.order_date") >= "2024-12-01"))
    .select(F.col("s.id"), F.col("s.order_id"), F.col("o.customer_id"))
    .orderBy("id")
)
