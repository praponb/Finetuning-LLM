from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("monthly_fee") > 60) & (F.col("status") == "ACTIVE")))
    .select(
        "plan",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)
