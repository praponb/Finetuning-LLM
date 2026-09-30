from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([10, 19, 26, 38]))
    .select("end_date", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("end_date", "customer_id")
    )
    .distinct()
)
