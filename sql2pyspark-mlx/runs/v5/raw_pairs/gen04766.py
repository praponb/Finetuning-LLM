from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("email").like("%@startup.io")).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.channel").isin(["partner", "web", "store"]))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)
