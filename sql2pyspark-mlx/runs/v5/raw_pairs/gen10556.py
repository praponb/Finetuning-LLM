from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["LOAN", "CHECKING", "CREDIT"])
    .agg(F.count("balance"))
)
