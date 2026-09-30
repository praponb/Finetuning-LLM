from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["segment", "plan"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("num_rows").desc())
)
