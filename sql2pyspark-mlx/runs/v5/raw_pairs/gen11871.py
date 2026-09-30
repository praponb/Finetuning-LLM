from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(170, 200) | F.month("start_date").isin([2, 3, 10]))
    .select("id", "status", "monthly_fee")
)
