from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]) | F.col("customer_id").isin([10, 16]))
    .select("id", "balance", "customer_id")
    .orderBy(F.col("balance").desc(), F.col("id").desc())
)
