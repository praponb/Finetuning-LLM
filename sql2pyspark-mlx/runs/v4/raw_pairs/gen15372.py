from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.signup_date").between("2023-03-01", "2025-05-01"))
    .select(F.col("c.name"), F.col("c.email"), F.col("o.customer_id"))
    .orderBy("email", "customer_id")
)
