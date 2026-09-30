from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.is_active"), F.col("c.country"))
    .orderBy(F.col("country").desc())
)
