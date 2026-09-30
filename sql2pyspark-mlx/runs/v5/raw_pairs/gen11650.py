from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .filter(F.col("discount") >= 0.15)
    .groupBy("customer_id")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.min_shipping_fee") > 35)
    .select(F.col("c.signup_date"), F.col("ct.min_shipping_fee"))
)
