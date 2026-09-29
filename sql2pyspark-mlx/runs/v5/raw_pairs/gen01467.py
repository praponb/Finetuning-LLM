from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 26)
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        F.round((F.col("monthly_fee") * 100), 2).alias("adjusted_monthly_fee"),
    )
    .orderBy("start_date")
)
