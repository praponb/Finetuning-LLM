from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 43000) & (F.col("balance") <= 40000) & F.col("balance").between(28000, 39000))
    .select("customer_id", "opened_date", "balance")
)
