from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("amount") <= 860)
    .groupBy("customer_id")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("shipping_fee").alias("unique_statuss"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.unique_statuss") >= 5)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("cs.last_order_date"),
        F.col("cs.unique_statuss"),
    )
    .orderBy("segment")
)
