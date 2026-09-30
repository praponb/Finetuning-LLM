from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]) & (F.col("balance") < 44000))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)
