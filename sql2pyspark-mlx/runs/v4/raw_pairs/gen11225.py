from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.month("end_date").isin([3, 8, 12]) & F.col("customer_id").isNull()) | (F.col("status") != "EXPIRED"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.date_sub("start_date", 81).alias("start_date_minus_81d"),
    )
    .orderBy("monthly_fee")
)
