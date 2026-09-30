from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("order_date") <= "2024-10-01") & F.col("store_id").isin([20, 21]))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

orders_stats = (
    recent_orders
    .groupBy("status", "channel")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
    )
)

result = (
    orders_stats
    .filter(F.col("shipped_amount") > 48)
    .select("status", "channel", "avg_amount", "shipped_amount")
    .orderBy("avg_amount")
)
