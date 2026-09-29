from pyspark.sql import functions as F

result = df_transactions.withColumn("amount_whole", F.floor("amount"))
