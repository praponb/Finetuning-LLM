from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", "plan")
    .distinct()
    .orderBy("plan", "status")
    .limit(10)
)
