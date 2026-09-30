from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([12, 20, 26, 31]) & F.col("merchant").isin(["Shell", "Lazada", "Amazon"]))
    .select("id", "amount", "merchant")
)
