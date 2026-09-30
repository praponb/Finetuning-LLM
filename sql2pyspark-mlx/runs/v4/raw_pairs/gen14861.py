from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("status") == "RETURNED") & (F.col("status") != "SHIPPED"))
    .withColumn("discount_level", F.when(F.col("discount") >= 0.41, "High").when(F.col("discount") >= 0.15, "Medium").otherwise("Low"))
)

summary = (
    recent_orders
    .groupBy("channel")
    .agg(F.min("order_date").alias("first_order_date"))
)

result = (
    summary
    .select("channel", "first_order_date")
    .orderBy("first_order_date", "channel")
)
