from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") <= "2023-08-01") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)
