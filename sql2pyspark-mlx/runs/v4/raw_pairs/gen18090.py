from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium"]))
    .select(
        "id",
        "end_date",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)
