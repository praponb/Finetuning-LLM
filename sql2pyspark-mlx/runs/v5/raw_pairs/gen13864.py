from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard", "premium"]))
    .select(
        "monthly_fee",
        F.add_months("end_date", 12).alias("end_date_plus_months"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
