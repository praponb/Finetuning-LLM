from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2024-10-21")
    .select("plan")
    .distinct()
    .orderBy(F.col("plan").desc())
)
