from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.amount").between(1110, 1150))
    .select(
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        (F.col("o.amount") * F.col("o.discount")).alias("amount_times_discount"),
    )
    .orderBy("discount")
)
