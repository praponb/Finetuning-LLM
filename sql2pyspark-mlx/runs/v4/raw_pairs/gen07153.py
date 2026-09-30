from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["web", "partner", "mobile", "store"])
    .agg(F.max("shipping_fee"))
    .orderBy("status")
)
