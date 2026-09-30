from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.country").alias("country"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
