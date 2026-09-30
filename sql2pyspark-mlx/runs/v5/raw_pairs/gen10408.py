from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "LOAN"])
    .agg(F.max("balance"))
    .orderBy("status")
)
