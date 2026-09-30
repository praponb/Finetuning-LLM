from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customer_stats.latest_order_date"),
    )
    .orderBy(F.col("signup_date").desc())
)
