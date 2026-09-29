from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")

result = (
    df_accounts
    .withColumn("max_balance_by_account_type", F.max("balance").over(w))
    .filter(F.col("balance") == F.col("max_balance_by_account_type"))
    .select("id", "account_type", "balance", "opened_date")
)
