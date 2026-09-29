from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .select("plan")
    .distinct()
    .orderBy("plan")
    .limit(10)
)
