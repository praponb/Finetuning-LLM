from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard", "premium"]) & F.col("monthly_fee").between(170, 230))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)
