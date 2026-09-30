from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "SAVINGS"])
    .agg(F.sum("balance"))
    .orderBy("status")
)
