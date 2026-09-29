from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 90)
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_customers") > 7)
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.city"),
        F.col("t.monthly_fee_range"),
        F.col("t.unique_customers"),
    )
    .orderBy(F.col("city").desc())
)
