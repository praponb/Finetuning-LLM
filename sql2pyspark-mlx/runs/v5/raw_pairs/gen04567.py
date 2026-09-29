from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "PAUSED"]))
    .select("plan")
    .unionByName(df_subscriptions.filter(F.col("customer_id").isNull()).select("plan"))
)
