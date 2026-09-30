from pyspark.sql import functions as F

max_balance = df_accounts.filter(F.col("balance").between(29000, 61000)).agg(F.max("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") > max_balance)
    .select("id", "balance", "opened_date")
    .orderBy("balance", F.col("id").desc())
    .limit(50)
)
