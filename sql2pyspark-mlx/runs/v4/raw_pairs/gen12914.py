from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(120, 150))
    .select("plan", "status")
    .distinct()
    .orderBy("plan", "status")
    .limit(5)
)
