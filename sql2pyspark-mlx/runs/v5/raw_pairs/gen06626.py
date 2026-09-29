from pyspark.sql import functions as F

result = df_transactions.select("amount", "txn_type", "account_id")
