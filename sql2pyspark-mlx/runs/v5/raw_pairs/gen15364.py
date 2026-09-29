from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(F.col("c.signup_date"), F.col("c.credit_limit"), F.col("o.shipping_fee"))
)
