from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id") == 15)
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

result = (
    cleaned
    .select("plan", "id")
    .subtract(
        df_subscriptions
        .filter(F.year("start_date") >= 2024)
        .select("plan", "id")
    )
)
