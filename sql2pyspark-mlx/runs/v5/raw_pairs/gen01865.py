from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)
