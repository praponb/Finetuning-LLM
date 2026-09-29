from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-08-01")
    .select("plan")
    .distinct()
    .orderBy("plan")
)
