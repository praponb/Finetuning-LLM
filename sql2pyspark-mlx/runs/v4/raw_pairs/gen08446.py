from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.filter(F.col("shipping_fee") < 18).alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left_anti")
    .select(
        F.col("customers.id"),
        F.col("customers.age"),
        F.col("customers.name"),
        F.col("customers.credit_limit"),
    )
)
