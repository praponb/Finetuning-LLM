from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 61, "high").when(F.col("c.age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.min(F.col("o.order_date")).alias("first_order_date"))
    .orderBy("first_order_date")
)
