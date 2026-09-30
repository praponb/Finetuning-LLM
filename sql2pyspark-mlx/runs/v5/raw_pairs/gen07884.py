from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .withColumn("order_date_plus_months", F.add_months("order_date", 3))
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 2))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

result = (
    base
    .groupBy("channel", "customer_id")
    .pivot("status", ["PENDING", "CANCELLED", "SHIPPED"])
    .agg(F.max("shipping_fee").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
