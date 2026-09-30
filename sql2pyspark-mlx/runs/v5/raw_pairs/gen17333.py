from pyspark.sql import functions as F

recent_orders = df_orders.filter(~(F.col("discount").isNotNull() & F.col("store_id").isNull()))

agg = (
    recent_orders
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    agg
    .filter(F.col("total_discount") >= 2.3)
    .orderBy(F.col("total_discount").desc(), "channel")
    .limit(5)
)
