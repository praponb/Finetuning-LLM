from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(df_payments.select(F.lit("payment").alias("origin"), "status"))
    .orderBy(F.col("status").desc())
)
