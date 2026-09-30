from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 29) & (F.col("start_date") <= "2024-07-01"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
    .orderBy("start_date")
)
