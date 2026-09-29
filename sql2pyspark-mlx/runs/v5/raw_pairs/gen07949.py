from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
)
