from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-05-20"))) & (F.col("customer_id") == 4))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("plan", ["enterprise", "premium", "standard"])
    .agg(F.max("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
