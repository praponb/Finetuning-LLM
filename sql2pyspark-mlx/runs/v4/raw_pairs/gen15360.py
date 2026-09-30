from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isin([23, 29, 39]) & (F.col("start_date") > "2024-07-01"))
    .groupBy("status")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .orderBy(F.col("monthly_fee_range").desc(), "status")
)
