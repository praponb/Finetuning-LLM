from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-07-27", "2025-02-16"))
    .select(
        "id",
        "end_date",
        "start_date",
        "monthly_fee",
        F.substring("plan", 1, 3).alias("plan_prefix"),
        F.add_months("start_date", 12).alias("start_date_plus_months"),
    )
)
