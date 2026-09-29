from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~((F.col("discount") < 0.19) & (F.col("shipping_fee") > 34)))
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") >= 0.36)
    .orderBy("avg_discount", "channel")
    .limit(50)
)
