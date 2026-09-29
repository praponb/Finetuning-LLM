from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 200))
    .select("id", "start_date", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["premium", "enterprise", "standard"]))
        .select("id", "start_date", "status")
    )
    .distinct()
    .orderBy("status")
)
