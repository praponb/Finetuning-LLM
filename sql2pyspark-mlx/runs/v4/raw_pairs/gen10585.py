from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("status", ["RETURNED", "COMPLETED", "PENDING"])
    .agg(F.sum("amount"))
)
