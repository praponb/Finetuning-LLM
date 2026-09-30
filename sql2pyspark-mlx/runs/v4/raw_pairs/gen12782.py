from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("opened_date_minus_84d", F.date_sub("opened_date", 84))
    .orderBy(F.col("customer_id").desc(), "account_type")
)
