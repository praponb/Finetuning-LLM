from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "PAUSED"]) & ~F.col("plan").isin(["premium", "enterprise", "basic"]))
    .orderBy("monthly_fee", "id")
    .limit(10)
)
