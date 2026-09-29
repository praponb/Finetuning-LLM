from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .select("id", "start_date", "plan")
    .orderBy(F.col("id").desc())
    .limit(1)
)
