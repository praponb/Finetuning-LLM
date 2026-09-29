from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.total_monthly_fee") > 1090)
    .select(F.col("c.city"), F.col("ct.total_monthly_fee"))
    .orderBy(F.col("total_monthly_fee").desc(), "city")
)
