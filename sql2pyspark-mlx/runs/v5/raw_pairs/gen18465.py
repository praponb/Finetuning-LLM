from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(((F.col("amount") <= 700) & F.col("customer_id").isNull()) | F.col("customer_id").isNotNull())
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
)

summary = (
    recent_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "channel")
    .agg(F.min("amount_with_shipping").alias("min_amount_with_shipping"))
)

result = (
    summary
    .filter(F.col("min_amount_with_shipping") >= 56)
    .orderBy(F.col("min_amount_with_shipping").desc())
)
