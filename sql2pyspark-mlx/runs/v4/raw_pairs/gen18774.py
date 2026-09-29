from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("customer_id").isin([2, 13]) | F.col("customer_id").isNull())

agg = (
    filtered_orders
    .groupBy(
        F.when(F.col("amount") >= 1570, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
)

result = (
    agg
    .orderBy("store_count", "amount_bucket")
    .limit(1)
)
