from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(130, 230))
    .select("status", "plan")
    .distinct()
)
