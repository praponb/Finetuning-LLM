from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("o.status") == "PENDING", F.col("o.amount")).otherwise(0)).alias("pending_amount"),
    )
    .filter(F.col("pending_amount") > 51)
)
