from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.count("balance"))
    .orderBy("account_type")
)
