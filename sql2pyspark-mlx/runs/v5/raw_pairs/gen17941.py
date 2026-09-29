from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("shipping_fee") > 16).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.col("c.city") == "Bangkok")
    .select(F.col("c.id"), F.col("c.email"))
)
