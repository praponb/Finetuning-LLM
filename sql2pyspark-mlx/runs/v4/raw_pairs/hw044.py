from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = df_transactions.select("account_id", "txn_time", "amount", F.sum("amount").over(w).alias("running_total"))
