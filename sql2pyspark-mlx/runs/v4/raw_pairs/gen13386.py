from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "CANCELLED"]))
    .select("plan", "start_date")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "EXPIRED"]))
        .select("plan", "start_date")
    )
    .orderBy("start_date")
)
