from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "LOAN"])
    .agg(F.count("balance"))
    .orderBy("customer_id")
)
