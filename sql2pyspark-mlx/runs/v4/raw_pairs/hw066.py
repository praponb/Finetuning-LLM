from pyspark.sql import functions as F

result = df_transactions.select("id", F.date_trunc("month", "txn_time").alias("txn_month"))
