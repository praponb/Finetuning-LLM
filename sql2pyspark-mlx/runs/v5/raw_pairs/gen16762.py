from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee") > 110)
        .select("customer_id")
    )
)
