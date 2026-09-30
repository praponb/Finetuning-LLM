from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([26, 33, 36]) & (F.col("plan") == "standard"))
    .select(
        "plan",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
    .orderBy("plan")
)
