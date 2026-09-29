from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") == "London")
    .select(
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("c.name"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
