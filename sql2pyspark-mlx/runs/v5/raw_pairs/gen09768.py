from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("balance_adjusted", F.round((F.col("balance") * 100), 2))
    .orderBy("opened_date", F.col("account_type").desc())
)
