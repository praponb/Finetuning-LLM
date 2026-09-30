from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.col("o.discount") >= 0.22) & F.lower(F.col("c.name")).like("%mma%"))
    .select(F.col("c.country"), F.col("o.customer_id"))
    .orderBy("country", "customer_id")
)
