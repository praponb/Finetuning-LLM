from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS", "CHECKING", "LOAN"])
    .agg(F.avg("balance"))
    .orderBy("status")
)
