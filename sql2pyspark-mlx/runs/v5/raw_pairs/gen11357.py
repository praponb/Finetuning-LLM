from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(46000, 48000))
    .select("id", "account_type", "status")
    .orderBy(F.col("account_type").desc())
)
