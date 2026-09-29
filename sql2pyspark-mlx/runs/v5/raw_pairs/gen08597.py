from pyspark.sql import functions as F

rollup_totals = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .cube(F.col("c.segment").alias("segment"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
)

result = rollup_totals.select("segment", "status", "min_monthly_fee")
