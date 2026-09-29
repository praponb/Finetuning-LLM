from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.shipping_fee") > 15)
    .select(
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("customers.country"),
        F.col("customers.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
