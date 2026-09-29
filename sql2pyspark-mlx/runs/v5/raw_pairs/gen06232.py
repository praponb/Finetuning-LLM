from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.age"),
        F.col("c.city"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("age").desc())
)
