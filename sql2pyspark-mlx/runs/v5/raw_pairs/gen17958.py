from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 5) & (F.col("plan") == "enterprise"))
    .select(
        "start_date",
        "plan",
        "monthly_fee",
        F.upper("plan").alias("plan_upper"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)
