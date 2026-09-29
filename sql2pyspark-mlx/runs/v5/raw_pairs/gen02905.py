from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & F.col("customer_id").isin([1, 30]))
    .select("account_type", "status")
    .orderBy("account_type", "status")
)
