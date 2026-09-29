from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .rollup(F.col("c.segment").alias("segment"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("src.monthly_fee")).alias("avg_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
