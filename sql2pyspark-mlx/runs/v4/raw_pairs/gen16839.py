from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.min("end_date").alias("first_end_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.city"), F.col("c.email"), F.col("t.first_end_date"))
    .orderBy(F.col("city").desc())
)
