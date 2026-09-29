from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 13)
    .select("account_id")
    .unionByName(df_transactions.filter(F.col("is_flagged")).select("account_id"))
    .orderBy("account_id")
)
