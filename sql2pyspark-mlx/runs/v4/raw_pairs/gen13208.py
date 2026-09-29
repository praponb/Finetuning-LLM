from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.monthly_fee_range") > 31)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.age"),
        F.col("sbc.monthly_fee_range"),
    )
    .orderBy(F.col("id").desc(), "country")
)
