from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") >= 2025)
    .select("end_date", F.trim("plan").alias("clean_plan"))
)
