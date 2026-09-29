from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") < "2025-04-01")
    .cube("status", "plan")
    .agg(F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"))
)
