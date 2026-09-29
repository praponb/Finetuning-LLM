from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "CREDIT", "LOAN"])
    .agg(F.sum("balance").alias("total"))
)
