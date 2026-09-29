from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]) & F.col("balance").between(52000, 57000))
    .groupBy("account_type", "customer_id")
    .agg(F.min("opened_date").alias("first_opened_date"))
)
