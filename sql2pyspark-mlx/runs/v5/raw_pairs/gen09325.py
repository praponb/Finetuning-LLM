from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.credit_limit").isNotNull() | (F.col("c.age") != 61))
    .select(
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("o.channel"),
        F.col("o.store_id"),
        (F.col("o.amount") / F.nullif(F.col("o.shipping_fee"), F.lit(0))).alias("amount_per_shipping_fee"),
    )
)
