from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(F.min("order_date").alias("min_order_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.col("cs.min_order_date"),
    )
    .orderBy(F.col("min_order_date").desc(), "id")
    .limit(10)
)
