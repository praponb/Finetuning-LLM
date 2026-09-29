from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 22]))
    .select("start_date")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([21, 22, 33, 38]))
        .select("start_date")
    )
)
