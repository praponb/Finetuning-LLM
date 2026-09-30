from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 180)
    .select("customer_id")
    .unionByName(
        df_subscriptions
        .filter(~F.col("plan").isin(["premium", "enterprise", "standard"]))
        .select("customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
