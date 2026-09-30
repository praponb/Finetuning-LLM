from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .groupBy("status")
    .pivot("channel", ["web", "store", "mobile"])
    .agg(F.sum("amount"))
    .orderBy("status")
)
