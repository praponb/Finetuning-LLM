from pyspark.sql import functions as F

result = df_transactions.filter(F.col("account_id").isin([2, 5]))
