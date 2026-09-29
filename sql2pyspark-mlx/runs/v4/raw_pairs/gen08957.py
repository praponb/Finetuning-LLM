from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["web", "mobile", "partner"])
    .agg(F.count("shipping_fee"))
    .orderBy("status")
)
