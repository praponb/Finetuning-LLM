from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("s.shipped_date") > "2023-07-01")
    .select(F.col("s.id"), F.col("s.status"), F.col("o.customer_id"))
)
