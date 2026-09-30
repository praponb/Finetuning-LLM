from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("customer_id").isin([3, 25, 27, 39]) & (F.year("start_date") == 2023) & F.col("start_date").between("2024-03-01", "2024-09-14")))
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
