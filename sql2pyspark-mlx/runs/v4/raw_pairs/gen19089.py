from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 18) & (F.col("opened_date") >= "2024-06-01"))
    .select("account_type")
    .orderBy("account_type")
)
