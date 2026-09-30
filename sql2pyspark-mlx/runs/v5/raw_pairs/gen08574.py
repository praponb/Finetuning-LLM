from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
)
