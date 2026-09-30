from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "country", "plan"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
        F.col("s.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
    )
)
