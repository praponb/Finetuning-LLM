from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.min_monthly_fee") > 120)
    .select(
        F.col("c.email"),
        F.col("c.segment"),
        F.col("C.min_monthly_fee"),
        F.col("C.unique_statuss"),
    )
)
