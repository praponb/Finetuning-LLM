from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.plan").isin(["standard", "premium"]))
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.count(F.when(F.col("s.status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") > 5)
    .orderBy("city")
)
