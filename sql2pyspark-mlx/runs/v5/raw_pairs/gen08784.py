from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "plan"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("s.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
