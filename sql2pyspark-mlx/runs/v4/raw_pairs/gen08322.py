from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([13, 14, 18, 40]) & (F.col("monthly_fee") < 120))
    .select("start_date")
    .orderBy(F.col("start_date").desc())
)
