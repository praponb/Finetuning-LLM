from pyspark.sql import functions as F

base = df_subscriptions.filter(F.col("status") == "CANCELLED")

agg = (
    base
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
)

result = agg
