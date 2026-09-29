from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise", "basic"]))
    .select("id", "end_date", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
        .select("id", "end_date", "plan")
    )
    .distinct()
)
