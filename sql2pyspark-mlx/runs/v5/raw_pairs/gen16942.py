from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount")

result = df_transactions.select("id", "txn_type", "amount", F.rank().over(w).alias("rnk"))
