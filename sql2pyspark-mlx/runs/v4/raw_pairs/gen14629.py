from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("shipments.order_id").isNotNull())
    .groupBy(
        F.when(F.col("customers.age") >= 60, "high").when(F.col("customers.age") >= 42, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.min(F.col("shipments.cost")).alias("min_cost"),
        F.sum(F.when(F.col("shipments.carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
    )
)
