from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("customers.country").isin(["JP", "UK", "TH"]) & (F.col("customers.country") != "UK"))
    .select(
        F.col("customers.email"),
        F.col("customers.credit_limit"),
        F.col("customers.country"),
        F.col("orders.order_date"),
        F.col("orders.shipping_fee"),
    )
)
