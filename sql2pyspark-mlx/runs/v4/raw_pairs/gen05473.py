from pyspark.sql import functions as F

result = df_accounts.filter(F.col("opened_date") >= "2025-08-01")
