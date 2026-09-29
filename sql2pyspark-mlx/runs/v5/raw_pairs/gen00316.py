from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 710) & (F.col("amount") < 700) & F.col("customer_id").isNotNull())
    .select("discount", "channel", F.upper("channel").alias("channel_upper"))
    .orderBy("channel_upper")
)
