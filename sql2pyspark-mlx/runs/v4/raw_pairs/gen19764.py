from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("customer_id").isin([9, 24, 25, 36]))
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.web_count") >= 9)
    .select(
        F.col("customers.is_active"),
        F.col("t.web_count"),
        F.col("t.first_order_date"),
    )
)
