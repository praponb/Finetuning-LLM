from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
