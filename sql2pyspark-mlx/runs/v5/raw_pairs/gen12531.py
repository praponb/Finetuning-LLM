from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 80)
    .select("status", "plan")
    .distinct()
)
