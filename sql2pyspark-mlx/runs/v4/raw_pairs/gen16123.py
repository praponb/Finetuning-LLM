from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & ((F.col("monthly_fee") < 200) | F.col("plan").isin(["enterprise", "premium"])))
)

summary = (
    recent_subscriptions
    .groupBy("plan", F.year("start_date").alias("start_year"))
    .agg(F.count("*").alias("row_count"))
)

result = summary.orderBy("row_count")
