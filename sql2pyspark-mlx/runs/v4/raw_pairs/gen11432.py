from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Lazada"]))
    .select("account_id")
    .unionByName(df_transactions.filter(F.col("amount") <= 1800).select("account_id"))
    .orderBy("account_id")
)
