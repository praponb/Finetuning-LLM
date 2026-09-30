from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 2700) & F.col("merchant").isin(["Lazada", "7-Eleven"]))
    .select("account_id", "amount", "merchant")
)
