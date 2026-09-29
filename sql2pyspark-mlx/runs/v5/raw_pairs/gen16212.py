from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.status")))).alias("status_values"),
    )
)
