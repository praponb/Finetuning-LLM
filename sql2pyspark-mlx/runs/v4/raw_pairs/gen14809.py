from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.month("end_date").isin([3, 9, 10]))
    .groupBy("customer_id")
    .agg(F.min("start_date").alias("first_start_date"))
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.credit_limit"),
        F.col("customer_summary.first_start_date"),
    )
)
