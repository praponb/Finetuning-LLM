from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise"]))
    .select("id", "start_date", "status")
    .orderBy(F.col("id").desc())
)
