from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .pivot("status", ["CANCELLED", "PENDING"])
    .agg(F.avg("shipping_fee"))
)
