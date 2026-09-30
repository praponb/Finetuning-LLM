from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .rollup(F.col("dim.segment").alias("segment"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
