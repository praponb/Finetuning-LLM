from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("customer_id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(100, 130))
        .select("customer_id", "plan")
    )
)
