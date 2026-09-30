from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") <= "2023-09-01")
    .select(
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
)
