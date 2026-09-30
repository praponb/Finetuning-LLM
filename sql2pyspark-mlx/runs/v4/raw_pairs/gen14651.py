from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-05-19", "2025-06-01"))
    .select("start_date", "plan")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "PAUSED", "EXPIRED"]))
        .select("start_date", "plan")
    )
    .orderBy("plan")
)
