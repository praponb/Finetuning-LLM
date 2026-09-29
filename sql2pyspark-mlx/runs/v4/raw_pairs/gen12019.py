from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 34) | (F.col("discount") >= 0.31))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.max("discount").alias("max_discount"),
    )
    .orderBy(F.col("shipping_fee_count").desc())
)
