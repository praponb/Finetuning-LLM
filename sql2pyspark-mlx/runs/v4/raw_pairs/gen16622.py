from pyspark.sql import functions as F

base = df_orders.filter(F.col("customer_id").isNotNull())

channel_summary = (
    base
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    channel_summary
    .select("channel", "latest_order_date", "first_order_date")
    .orderBy("latest_order_date", F.col("first_order_date").desc())
)
