from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "LOAN", "CREDIT", "SAVINGS"])
    .agg(F.count("balance"))
)
