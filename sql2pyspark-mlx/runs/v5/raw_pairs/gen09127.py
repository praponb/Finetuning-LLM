from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") <= "2025-02-01"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

result = (
    filtered
    .select("customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") >= "2023-10-01")
        .select("customer_id")
    )
)
