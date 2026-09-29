from pyspark.sql import functions as F

result = df_transactions.select("id", "account_id", "is_flagged")
