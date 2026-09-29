from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.country").alias("country"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
    )
)
