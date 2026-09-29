from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.age").between(49, 55))
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(F.min(F.col("shipments.cost")).alias("min_cost"))
    .filter(F.col("min_cost") >= 74)
    .orderBy("min_cost")
)
