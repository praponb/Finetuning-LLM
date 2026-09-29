from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise", "basic"]))
    .select(
        "plan",
        "customer_id",
        "start_date",
        F.date_format("end_date", "yyyy-MM-dd").alias("end_date_label"),
    )
)
