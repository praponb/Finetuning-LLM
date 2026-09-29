from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & (F.col("balance") <= 39000) & F.month("opened_date").isin([1, 2, 6]))
    .select("status", "customer_id", F.col("balance").cast("int").alias("balance_int"))
)
