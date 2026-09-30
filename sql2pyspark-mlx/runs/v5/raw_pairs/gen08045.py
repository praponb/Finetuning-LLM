from pyspark.sql import functions as F

avg_balance = df_accounts.agg(F.avg("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") > avg_balance)
    .select("id", "balance", "opened_date", "status")
    .orderBy("id")
)
