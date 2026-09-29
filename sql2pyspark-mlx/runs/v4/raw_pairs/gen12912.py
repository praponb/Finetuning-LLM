from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.channel").isin(["mobile", "partner", "store"]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("shipments.cost"),
        F.col("orders.discount"),
        F.col("orders.amount"),
        F.col("customers.name"),
        (F.col("customers.credit_limit") - F.col("customers.age")).alias("credit_limit_minus_age"),
    )
    .orderBy("cost", "id")
    .limit(10)
)
