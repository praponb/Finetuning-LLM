from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .pivot("plan", ["premium", "enterprise"])
    .agg(F.count("monthly_fee"))
)
