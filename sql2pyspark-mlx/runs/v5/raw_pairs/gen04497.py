from pyspark.sql import functions as F

result = df_transactions.select("id", "txn_type")
