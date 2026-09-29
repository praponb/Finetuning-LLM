from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.shipping_fee").between(35, 38) & (F.year(F.col("c.signup_date")) >= 2024))
    .select(F.col("o.customer_id"), F.col("o.amount"), F.col("c.name"))
)
