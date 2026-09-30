from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("customer_id").isin([16, 19, 21, 27]) & F.col("customer_id").isin([9, 20])))
    .withColumn("start_date_label", F.date_format("start_date", "MMM yyyy"))
    .orderBy(F.col("monthly_fee").desc(), F.col("status").desc())
)
