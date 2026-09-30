from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .rollup(
        F.col("c.country").alias("country"),
        F.col("c.city").alias("city"),
        F.col("e.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
