from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "customer_id")
    .pivot("channel", ["partner", "store"])
    .agg(F.sum("shipping_fee"))
)
