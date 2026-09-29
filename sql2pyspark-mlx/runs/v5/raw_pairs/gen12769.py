from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store", "partner"]) & F.col("shipping_fee").between(14, 33))
    .select("id", "channel", F.upper("channel").alias("channel_upper"))
)
