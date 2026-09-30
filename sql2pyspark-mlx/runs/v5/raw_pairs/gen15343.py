from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") >= "2024-01-01") & F.col("amount").between(550, 940))
    .withColumn("amount_level", F.when(F.col("amount") >= 1310, "High").when(F.col("amount") >= 480, "Medium").otherwise("Low"))
)

channel_summary = (
    base
    .groupBy("channel")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
)

result = channel_summary.select("channel", "discount_range")
