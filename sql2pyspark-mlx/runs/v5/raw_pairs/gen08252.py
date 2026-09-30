from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNotNull())
    .select("id", "end_date", F.round("monthly_fee", 2).alias("monthly_fee_rounded"))
)
