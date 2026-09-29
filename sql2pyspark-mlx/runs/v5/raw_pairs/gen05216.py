from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("s.carrier") == "UPS", F.col("s.cost")).otherwise(0)).alias("ups_cost"),
    )
)
