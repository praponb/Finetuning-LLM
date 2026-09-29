from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("channel").isNull() & F.col("customer_id").isNotNull() & F.col("customer_id").isNull())
    .withColumn("discount_filled", F.coalesce("discount", F.lit(0)))
)

orders_stats = (
    valid_orders
    .groupBy(F.month("order_date").alias("order_month"), "status")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    orders_stats
    .filter(F.col("min_amount") > 1210)
    .select("order_month", "status", "min_amount")
    .orderBy("status")
)
