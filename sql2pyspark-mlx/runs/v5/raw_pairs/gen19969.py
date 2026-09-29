from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .pivot("channel", ["web", "store", "partner", "mobile"])
    .agg(F.avg("amount"))
)
