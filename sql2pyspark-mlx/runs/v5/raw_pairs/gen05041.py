from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.stddev_monthly_fee") >= 57)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.country"),
        F.col("C.stddev_monthly_fee"),
    )
)
