from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.round("monthly_fee", 0).alias("monthly_fee_rounded"))
)

result = (
    filtered
    .select("plan", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") > "2025-05-08")
        .select("plan", "status")
    )
)
