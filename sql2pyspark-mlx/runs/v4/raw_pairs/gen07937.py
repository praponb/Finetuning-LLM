from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("store_id").isNotNull()).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.is_active"))
    .orderBy(F.col("id").desc())
)
