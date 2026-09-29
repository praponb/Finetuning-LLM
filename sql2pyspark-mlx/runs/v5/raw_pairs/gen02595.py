from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.sum(F.when(F.col("s.status") == "CANCELLED", F.col("s.monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
    )
)
