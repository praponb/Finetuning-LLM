from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.sqft") != 9762)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("s.store_name"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
)
