from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "CANCELLED") & F.col("customer_id").isin([7, 14]) & F.col("status").isin(["PENDING", "CANCELLED", "RETURNED"]))
    .select("id", "discount", F.length("channel").alias("channel_length"))
)
