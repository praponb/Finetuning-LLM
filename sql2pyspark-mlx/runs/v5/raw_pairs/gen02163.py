from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount").between(410, 500))
    .select(
        F.col("o.status"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("c.city"),
    )
    .orderBy(F.col("customer_id").desc())
)
