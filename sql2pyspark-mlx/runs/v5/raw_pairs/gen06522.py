from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([5, 29, 40]) & (F.col("opened_date") <= "2024-04-01"))
    .select("id", "status", "customer_id", "account_type")
    .orderBy("id")
)
