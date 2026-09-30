from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.countDistinct("customer_id").alias("unique_customers"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("ct.unique_customers"),
    )
    .orderBy("country", "id")
    .limit(3)
)
