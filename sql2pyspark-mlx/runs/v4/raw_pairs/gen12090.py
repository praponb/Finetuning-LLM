from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("start_date", "end_date")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee") <= 160)
        .select("start_date", "end_date")
    )
)
