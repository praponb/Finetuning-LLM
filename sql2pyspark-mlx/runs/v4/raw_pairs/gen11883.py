from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "EXPIRED"]))
    .select("start_date", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee") > 80)
        .select("start_date", "customer_id")
    )
    .distinct()
)
