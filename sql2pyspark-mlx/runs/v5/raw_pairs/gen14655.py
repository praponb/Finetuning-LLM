from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") <= "2023-08-01") | F.col("customer_id").isin([5, 20, 38]))
    .select("id", "status", "account_type", "balance")
    .orderBy(F.col("balance").desc())
)
