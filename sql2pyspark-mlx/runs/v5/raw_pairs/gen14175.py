from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["mobile", "store", "partner"])
    .agg(F.avg("amount"))
    .orderBy("status")
)
