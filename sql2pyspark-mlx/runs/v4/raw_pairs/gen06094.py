from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "partner", "store"]) & F.col("shipping_fee").between(11, 23) & F.month("order_date").isin([2, 4, 5]))
    .groupBy("channel", "customer_id")
    .agg(
        F.min("discount").alias("min_discount"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("min_discount", "total_amount")
)
