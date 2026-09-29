from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE"]))
    .select("id", "end_date", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["EXPIRED", "CANCELLED"]))
        .select("id", "end_date", "status")
    )
    .orderBy("id")
)
