from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("status") != "CANCELLED")
    .groupBy("customer_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.countDistinct("shipping_fee").alias("unique_channels"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_channels") > 10)
    .select(F.col("c.age"), F.col("t.min_amount"), F.col("t.unique_channels"))
)
