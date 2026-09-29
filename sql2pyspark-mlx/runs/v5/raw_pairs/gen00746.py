from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("id").desc())
)
