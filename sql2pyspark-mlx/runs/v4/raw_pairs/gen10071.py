from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-12-27")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.length("plan").alias("plan_length"),
    )
)

result = (
    base
    .groupBy("customer_id", "plan")
    .pivot("status", ["CANCELLED", "PAUSED"])
    .agg(F.avg("monthly_fee"))
)
