from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")

result = (
    df_accounts
    .withColumn("avg_balance_by_account_type", F.avg("balance").over(w))
    .filter(F.col("balance") < F.col("avg_balance_by_account_type"))
    .select("id", "account_type", "balance", "customer_id")
)
