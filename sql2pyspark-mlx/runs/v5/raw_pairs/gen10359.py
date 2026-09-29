from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")

result = (
    df_transactions
    .withColumn("min_amount_by_account", F.min("amount").over(w))
    .filter(F.col("amount") == F.col("min_amount_by_account"))
    .select("id", "account_id", "amount")
    .orderBy(F.col("account_id").desc())
)
