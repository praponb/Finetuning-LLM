from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_int", F.col("monthly_fee").cast("int"))
    .withColumn("start_date_period", F.date_trunc("week", "start_date"))
)
