from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    base
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.month("end_date").isin([2, 4, 5]))
        .select("status")
    )
)
